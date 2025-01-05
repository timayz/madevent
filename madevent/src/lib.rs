mod consumer;
mod cursor;
mod event;
mod producer;

//use futures::{stream, Stream};

pub use consumer::{Consumer, ConsumerError};
pub use cursor::{BindCursor, Cursor, Query, ToCursor};
pub use event::Event;
pub use producer::Producer;

/*pub struct MadEvent {
    name: String,
}

impl MadEvent {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self { name }
    }

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

    pub fn aggregate(&self, _value: impl Into<String>) -> Writer {
        todo!()
    }
}*/
