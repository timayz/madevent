use crate::Event;
use futures::{stream, Stream};

pub struct Reader {}

impl Reader {
    pub fn read(&self, aggregate: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub fn stream(&self, filter: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }
}
