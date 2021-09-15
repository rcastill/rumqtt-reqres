use bytes::Bytes;
use rumqttc::{AsyncClient, ClientError, Event, Incoming, Publish, QoS};

pub mod client;

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
        // TODO: Re-subscribe on reconnection
        let all_requests = format!("{}+", base_req_topic);
        client.subscribe(&all_requests, QoS::ExactlyOnce).await?;

        // Create new service
        Ok(Service {
            client: client.clone(),
            base_req_topic,
            base_res_topic: format!("{}/responses/", topic),
        })
    }

    pub fn parse_request(&self, event: &Event) -> Option<Responder> {
        match event {
            Event::Incoming(Incoming::Publish(Publish { topic, payload, .. })) => {
                get_endpoint_id(&self.base_req_topic, topic).map(|reqid| Responder {
                    client: self.client.clone(),
                    topic: format!("{}{}", self.base_res_topic, reqid),
                    payload: payload.clone(),
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
        for (test, expected) in test_pairs {
            assert_eq!(without_trailing_slashes(test), expected)
        }
    }
}
