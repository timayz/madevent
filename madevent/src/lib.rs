mod sender;

use futures::{stream, Stream};
use serde::Serialize;
use ulid::Ulid;

pub use sender::{Event, Sender};

pub struct MadEvent;

impl MadEvent {
    pub async fn read(
        &self,
        _aggregate_id: impl Into<String>,
        _first: u16,
        _after: Option<Ulid>,
    ) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub async fn stream(&self, _filter: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub async fn stream_all(&self, _filter: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub async fn stream_on_fly(&self, _filter: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub async fn stream_key_on_fly(&self, _filter: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub fn event(&self, _name: impl Into<String>, _data: impl Serialize) -> Sender {
        todo!()
    }

    pub fn event_with_metadata(
        &self,
        _name: impl Into<String>,
        _data: impl Serialize,
        _metadata: impl Serialize,
    ) -> Sender {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn read() {
        let madevt = MadEvent {};

        let mut reader = madevt.read("test-1", 1, None).await;
        assert_eq!(reader.next().await, None);
    }
}
