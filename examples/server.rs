use std::time::Duration;

use rumqtt_reqres::Service;
use rumqttc::{AsyncClient, MqttOptions};
use tokio::time::sleep;

#[tokio::main(flavor = "current_thread")]
async fn main() {
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
    loop {
        match eventloop.poll().await {
            Ok(ev) => {
                if let Some(res) = service.parse_request(&ev) {
                    tokio::spawn(res.respond_once(|_| "Hello World!"));
                }
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        };
    }
}
