use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use rumqttc::{AsyncClient, QoS};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

use crate::without_trailing_slashes;

pub struct Client {
    responders: Arc<Mutex<HashMap<String, oneshot::Sender<Bytes>>>>,
    base_req_topic: String,
    base_res_topic: String,
    mqtt: AsyncClient,
}

impl Client {
    pub async fn subscribe(
        base_topic: &str,
        mqtt: &AsyncClient,
    ) -> Result<Client, rumqttc::ClientError> {
        // clean base topic
        let topic = without_trailing_slashes(base_topic);
        let base_res_topic = format!("{}/responses/", topic);

        // Subscribe to requests
        // TODO: Re-subscribe on reconnection
        // TODO: almost same code in Client::subscribe/Service::subscribe
        let all_responses = format!("{}+", base_res_topic);
        mqtt.subscribe(&all_responses, QoS::ExactlyOnce).await?;

        // Create new service
        Ok(Client {
            responders: Arc::new(Mutex::new(HashMap::new())),
            mqtt: mqtt.clone(),
            base_req_topic: format!("{}/requests/", topic),
            base_res_topic,
        })
    }

    pub async fn set_response(&self, res_id: &str, payload: Bytes) -> bool {
        let mut set = false;
        if let Some(chan) = self.responders.lock().await.remove(res_id) {
            set = chan.send(payload).is_ok();
        }
        set
    }

    pub async fn send(&self, payload: impl Into<Vec<u8>>) {
        let (tx, rx) = oneshot::channel();
        let mut buf = Uuid::encode_buffer();
        let id = Uuid::new_v4().to_hyphenated().encode_lower(&mut buf);
        self.responders.lock().await.insert(id.to_owned(), tx);
        self.mqtt
            .publish(
                format!("{}{}", self.base_req_topic, id),
                QoS::ExactlyOnce,
                false,
                payload,
            )
            .await
            .unwrap();
        let res = tokio::time::timeout(timeout, rx).await.map(Result::unwrap);

        // if timeout, remove sender end
        if res.is_err() {
            self.responders.lock().await.remove(id);
        }
        res
    }
}
