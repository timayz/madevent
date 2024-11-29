use futures::stream::{self, Stream};
use serde::Serialize;

pub struct MadEvent;

impl MadEvent {
    pub async fn read(
        &self,
        _aggregate_id: impl Into<String>,
        _first: u16,
        _after: Option<Cursor>,
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

pub struct Sender;

impl Sender {
    pub fn event(self, _name: impl Into<String>, _data: impl Serialize) -> Self {
        todo!()
    }

    pub fn event_with_metadata(
        self,
        _name: impl Into<String>,
        _data: impl Serialize,
        _metadata: impl Serialize,
    ) -> Self {
        todo!()
    }

    pub async fn send(&self) -> Vec<SenderEvent> {
        todo!()
    }
}

pub struct Event;
pub struct SenderEvent;
pub struct Cursor;
