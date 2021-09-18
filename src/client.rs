use std::{collections::HashMap, fmt::Error, sync::Arc, time::Duration};

use bytes::Bytes;
use rumqttc::{AsyncClient, QoS};
use tokio::{
    sync::{oneshot, Mutex},
    time::error::Elapsed,
};
use uuid::Uuid;

use crate::without_trailing_slashes;

struct SendHelperError {
    id: String,
    error: Box<dyn std::error::Error>,
}

impl<E> From<(String, E)> for SendHelperError
where
    E: Into<Box<dyn std::error::Error>>,
{
    fn from((id, error): (String, E)) -> SendHelperError {
        SendHelperError {
            id,
            error: error.into(),
        }
    }
}

fn generate_uuid() -> String {
    let mut buf = Uuid::encode_buffer();
    Uuid::new_v4()
        .to_hyphenated()
        .encode_lower(&mut buf)
        .to_string()
}

pub struct Request<B> {
    id: String,
    body: B,
    client: Client,
    timeout: Option<Duration>,
}

impl<B> Clone for Request<B>
where
    B: Clone,
{
    fn clone(&self) -> Self {
        Request {
            id: generate_uuid(),
            body: self.body.clone(),
            client: self.client.clone(),
            timeout: self.timeout.clone(),
        }
    }
}

impl<B> Request<B>
where
    B: Into<Vec<u8>>,
{
    fn new(body: B, client: Client) -> Request<B> {
        Request {
            // Generate again so it does not collide with registry
            id: generate_uuid(),
            body,
            client,
            timeout: None,
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub async fn send(self) -> Result<Bytes, Elapsed> {
        let (tx, rx) = oneshot::channel();
        self.client
            .responders
            .lock()
            .await
            .insert(self.id.clone(), tx);
        self.client
            .mqtt
            .publish(
                format!("{}{}", self.client.base_req_topic, self.id),
                QoS::ExactlyOnce,
                false,
                self.body,
            )
            .await
            .unwrap();
        // .map_err(|e| (id.to_string(), e))?;

        // Result::unwrap should be safe: oneshot channel must remain open
        match self.timeout {
            Some(dur) => tokio::time::timeout(dur, rx).await.map(Result::unwrap),
            None => Ok(rx.await.unwrap()),
        }

        // Ok(tokio::time::timeout(timeout, rx)
        //     .await
        //     .map(Result::unwrap)
        //     .map_err(|e| (id.to_string(), e))?)
        // let res = tokio::time::timeout(timeout, rx).await.map(Result::unwrap);

        // // if timeout, remove sender end
        // if res.is_err() {
        //     self.responders.lock().await.remove(id);
        // }
        // Ok(res?)
    }
}

#[derive(Clone)]
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

    pub fn request<B>(&self, body: B) -> Request<B>
    where
        B: Into<Vec<u8>>,
    {
        Request::new(body, self.clone())
    }
}
