use std::{iter, time::Duration};

use futures::{stream, StreamExt};
use rumqtt_reqres::client::Client;
// use rumqtt_reqres::Service;
use rumqttc::{AsyncClient, MqttOptions};
use tokio::time::sleep;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    log::warn!("TODO: REMEMBER RECONNECTION LOGIC");

    // MQTT Options
    let mut opts = MqttOptions::new("mqtt-reqres-client", "localhost", 1883);
    opts.set_clean_session(true)
        .set_keep_alive(15)
        .set_connection_timeout(10);

    // Create client
    let (client, mut eventloop) = AsyncClient::new(opts, 10);
    let mut sub_attempts_remaining = 2;

    // Create service
    let cli = loop {
        match Client::subscribe("mqttservice/sample", &client).await {
            Ok(c) => break c,
            Err(e) => {
                eprintln!(
                    "Could not create client: {} [attempts remaining: {}]",
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

    // Prepare concurrent requests
    let mut clients = stream::iter(iter::repeat(cli.clone()).map(|cli| async move {
        eprintln!("Request");
        let res = cli
            .request("Heyoo!!")
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        match res {
            Ok(response) => eprintln!(
                "Response: {}",
                String::from_utf8(response.to_vec()).unwrap()
            ),
            Err(e) => {
                eprintln!("Could not get response: {:?}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }))
    .buffer_unordered(4);

    // Execute 4 at a time
    tokio::spawn(async move { while let Some(_) = clients.next().await {} });

    // Assign server responses
    loop {
        match eventloop.poll().await {
            Ok(ev) => {
                if let Some(resid) = cli.parse_response(&ev).await {
                    eprintln!("Response set for {}", resid);
                }
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        };
    }
}
