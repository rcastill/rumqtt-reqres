use bytes::Bytes;
use rumqttc::{
    AsyncClient, ClientError, ConnAck, ConnectReturnCode, Event, Incoming, Publish, QoS,
};

pub mod client;

pub struct SubscriptionHandle {
    topic: String,
    client: AsyncClient,
}

impl SubscriptionHandle {
    fn with_root(base_topic: &str, client: &AsyncClient) -> SubscriptionHandle {
        SubscriptionHandle {
            topic: format!("{}+", base_topic),
            client: client.clone(),
        }
    }

    pub async fn subscribe(&self) -> Result<(), rumqttc::ClientError> {
        log::info!("Subscribing to {}", self.topic);
        self.client.subscribe(&self.topic, QoS::ExactlyOnce).await
    }
}

async fn subscribe_if_connected(
    event: &Event,
    client: &AsyncClient,
    base_topic: &str,
) -> Option<SubscriptionHandle> {
    if let Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) = event {
        if code == &ConnectReturnCode::Success {
            return Some(SubscriptionHandle::with_root(base_topic, client));
        }
    }
    None
}

pub enum Reaction {
    Request(Responder),
    Subscribe(SubscriptionHandle),
}

pub fn get_endpoint_id<'topic>(base_topic: &str, topic: &'topic str) -> Option<&'topic str> {
    if topic.len() <= base_topic.len() || !topic.starts_with(&base_topic) {
        return None;
    }
    Some(&topic[base_topic.len()..])
}

fn without_trailing_slashes(input: &str) -> &str {
    // Border case
    if input.is_empty() {
        return input;
    }

    // In any other case, count trailing slashes
    let slash_count = input
        .chars()
        .rev()
        .map(|c| c == '/')
        .take_while(|&is_slash| is_slash)
        .count();
    &input[..input.len() - slash_count]
}

pub struct Service {
    client: AsyncClient,
    base_req_topic: String,
    base_res_topic: String,
}

impl Service {
    pub async fn subscribe(
        base_topic: &str,
        client: &AsyncClient,
    ) -> Result<Service, rumqttc::ClientError> {
        // clean base topic
        let topic = without_trailing_slashes(base_topic);
        let base_req_topic = format!("{}/requests/", topic);

        // Subscribe to requests
        SubscriptionHandle::with_root(&base_req_topic, client)
            .subscribe()
            .await?;

        // Create new service
        Ok(Service {
            client: client.clone(),
            base_req_topic,
            base_res_topic: format!("{}/responses/", topic),
        })
    }

    // pub async fn parse_request(&self, event: &Event) -> Option<Responder> {
    /// React to rumqtt event; either:
    ///
    /// - Detect connection, return Some(Reaction::Subscribe(SubscribeHandle))
    /// - Detect request, return Some(Reaction::Request(Responder))
    pub async fn react(&self, event: &Event) -> Option<Reaction> {
        let sub = subscribe_if_connected(event, &self.client, &self.base_req_topic)
            .await
            .map(Reaction::Subscribe);
        if sub.is_some() {
            return sub;
        }
        match event {
            Event::Incoming(Incoming::Publish(Publish { topic, payload, .. })) => {
                get_endpoint_id(&self.base_req_topic, topic).map(|reqid| {
                    Reaction::Request(Responder {
                        client: self.client.clone(),
                        topic: format!("{}{}", self.base_res_topic, reqid),
                        payload: payload.clone(),
                    })
                })
            }
            _ => None,
        }
    }
}

pub struct Responder {
    client: AsyncClient,
    topic: String,
    payload: Bytes,
}

impl Responder {
    pub async fn respond_once<F, B>(self, handler: F) -> Result<(), ClientError>
    where
        F: FnOnce(Bytes) -> B,
        B: Into<Vec<u8>>,
    {
        let payload = handler(self.payload);
        self.client
            .publish(&self.topic, QoS::ExactlyOnce, false, payload)
            .await
    }

    pub async fn respond<F, B>(&self, handler: F) -> Result<(), ClientError>
    where
        F: Fn(&[u8]) -> B,
        B: Into<Vec<u8>>,
    {
        let payload = handler(&self.payload);
        self.client
            .publish(&self.topic, QoS::ExactlyOnce, false, payload)
            .await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn trailing_slashes() {
        // test, expected
        let test_pairs = [
            ("some/normal/endpoint", "some/normal/endpoint"),
            ("some/normal/endpoint///", "some/normal/endpoint"),
            ("/", ""),
            ("////ads", "////ads"),
            ("//", ""),
            ("", ""),
            ("/only/one/", "/only/one"),
        ];
        for &(test, expected) in &test_pairs {
            assert_eq!(without_trailing_slashes(test), expected)
        }
    }
}
