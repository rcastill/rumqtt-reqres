use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};

use bytes::Bytes;
use rumqttc::{AsyncClient, ClientError, Event, Incoming, Publish, QoS};
use tokio::{
    sync::{oneshot, Mutex},
    time::error::Elapsed,
};
use uuid::Uuid;

use crate::{
    get_endpoint_id, subscribe_if_connected, without_trailing_slashes, SubscriptionHandle,
};

pub enum Reaction<'ev> {
    Response(&'ev str),
    Subscribe(SubscriptionHandle),
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

#[derive(Debug)]
pub enum SendError {
    ClientError(ClientError),
    Timeout,
}

impl From<ClientError> for SendError {
    fn from(c: ClientError) -> Self {
        SendError::ClientError(c)
    }
}

impl From<Elapsed> for SendError {
    fn from(_: Elapsed) -> Self {
        SendError::Timeout
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

    pub async fn send(self) -> Result<Bytes, SendError> {
        // Open response channel and register with client
        let (tx, rx) = oneshot::channel();
        self.client.register_responder(&self.id, tx).await;

        // Make send helper to act on possible errors (remove oneshot channel from registry)
        let body = self.body;
        let mqtt = self.client.mqtt.clone();
        let topic = format!("{}{}", self.client.base_req_topic, self.id);
        let timeout = self.timeout;
        let send = async move {
            mqtt.publish(topic, QoS::ExactlyOnce, false, body).await?;

            // Result::unwrap should be safe: oneshot channel must remain open
            Ok(match timeout {
                Some(dur) => tokio::time::timeout(dur, rx).await.map(Result::unwrap)?,
                None => rx.await.unwrap(),
            })
        };

        // Send request
        let result = send.await;

        // If request failed (connection -or- timeout), remove channel from registry
        if result.is_err() {
            self.client.remove_responder(&self.id).await;
        }
        result
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
        SubscriptionHandle::with_root(&base_res_topic, mqtt)
            .subscribe()
            .await?;

        // Create new service
        Ok(Client {
            responders: Arc::new(Mutex::new(HashMap::new())),
            mqtt: mqtt.clone(),
            base_req_topic: format!("{}/requests/", topic),
            base_res_topic,
        })
    }

    /// Method for traceability
    async fn register_responder(
        &self,
        id: impl Into<String> + Display,
        tx: oneshot::Sender<Bytes>,
    ) {
        let mut responders = self.responders.lock().await;
        log::debug!("Request registered: {}", id);
        responders.insert(id.into(), tx);
        log::trace!("{} active requests", responders.len());
    }

    /// Method for traceability
    async fn remove_responder(&self, id: &str) -> Option<oneshot::Sender<Bytes>> {
        let mut responders = self.responders.lock().await;
        let responder = responders.remove(id);
        if responder.is_some() {
            log::debug!("Request removed: {}", id);
        }
        log::trace!("{} active requests", responders.len());
        responder
    }

    async fn set_response(&self, res_id: &str, payload: Bytes) -> bool {
        let mut set = false;
        if let Some(chan) = self.remove_responder(res_id).await {
            set = chan.send(payload).is_ok();
        }
        set
    }

    /// Parse and set response if possible
    /// Return Some(request id) if it was possible
    /// React to rumqtt event; either:
    ///
    /// - Detect connection, return Some(Reaction::Subscribe(SubscribeHandle))
    /// - Detect response, return Some(Reaction::Response(request id))
    pub async fn react<'ev>(&self, event: &'ev Event) -> Option<Reaction<'ev>> {
        let sub = subscribe_if_connected(event, &self.mqtt, &self.base_res_topic)
            .await
            .map(Reaction::Subscribe);
        if sub.is_some() {
            return sub;
        }
        match event {
            Event::Incoming(Incoming::Publish(Publish { topic, payload, .. })) => {
                let mut maybe_res_id = get_endpoint_id(&self.base_res_topic, topic);
                if let Some(res_id) = maybe_res_id {
                    if !self.set_response(res_id, payload.clone()).await {
                        maybe_res_id = None;
                    }
                }
                maybe_res_id.map(Reaction::Response)
            }
            _ => None,
        }
    }

    pub fn request<B>(&self, body: B) -> Request<B>
    where
        B: Into<Vec<u8>>,
    {
        Request::new(body, self.clone())
    }
}
