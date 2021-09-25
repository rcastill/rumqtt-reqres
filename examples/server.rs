use std::{sync::Arc, time::Duration};

use rumqtt_reqres::{Reaction, Service};
use rumqttc::{AsyncClient, MqttOptions};
use tokio::{sync::Mutex, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // MQTT Options
    let mut opts = MqttOptions::new("mqtt-reqres-server", "localhost", 1883);
    opts.set_clean_session(true)
        .set_keep_alive(15)
        .set_connection_timeout(10);

    // Create client
    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let mut sub_attempts_remaining = 2;

    // Create service
    let service = loop {
        match Service::subscribe("mqttservice/sample", &client).await {
            Ok(s) => break s,
            Err(e) => {
                eprintln!(
                    "Could not create service: {} [attempts remaining: {}]",
                    e, sub_attempts_remaining
                );
                sub_attempts_remaining -= 1;
                if sub_attempts_remaining == 0 {
                    eprintln!("Could not subscribe to requests topics");
                    return;
                }
                sleep(Duration::from_secs(3)).await;
            }
        }
    };

    // Respond to requests
    // TODO: respond something meaningful
    let resubh = Arc::new(Mutex::new(None));
    loop {
        match eventloop.poll().await {
            Ok(ev) => match service.react(&ev).await {
                Some(Reaction::Request(res)) => {
                    log::info!("Got request");
                    tokio::spawn(res.respond_once(|_| "Hello World!"));
                }
                Some(Reaction::Subscribe(subh)) => {
                    if resubh.lock().await.is_some() {
                        log::debug!("Still trying to re-subscribe from previous iteration");
                        continue;
                    }
                    let container = resubh.clone();
                    *resubh.lock().await = Some(tokio::spawn(async move {
                        while let Err(e) = subh.subscribe().await {
                            log::error!("Cannot re-subscribe: {}", e);
                            sleep(Duration::from_secs(3)).await;
                        }
                        *container.lock().await = None;
                    }));
                }
                None => {}
            },
            Err(e) => {
                eprintln!("Connection error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        };
    }
}
