mod cursor;
mod event;
mod reader;
mod writer;

use futures::{stream, Stream};
use ulid::Ulid;

pub use cursor::{BindCursor, Cursor, ToCursor};
pub use event::Event;
pub type SqliteReader<'args, O> = Reader<'args, sqlx::Sqlite, O>;
pub use reader::Reader;
pub use writer::Writer;

#[allow(dead_code)]
pub struct MadEvent {
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

    pub fn aggregate(&self, _value: impl Into<String>) -> Writer {
        todo!()
    }
}
