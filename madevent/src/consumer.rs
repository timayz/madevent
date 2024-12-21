use crate::Event;
use futures::{stream, Stream};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("url: {0}")]
    Url(#[from] url::ParseError),
}

pub struct Consumer {
    path: String,
    persistent: bool,
}

impl Consumer {
    pub fn stream(filter: impl Into<String>) -> Result<impl Stream<Item = Event>, ConsumerError> {
        let filter = Url::parse(&filter.into())?;
        Ok(stream::iter(vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stream() {
        let consumer = Consumer::stream("non-persistent://*/article").unwrap();
        todo!()
    }

    #[tokio::test]
    async fn stream_use_ns() {
        let consumer = Consumer::stream("non-persistent://eu-west-1/article").unwrap();
        todo!()
    }

    #[tokio::test]
    async fn stream_persistent() {
        let consumer = Consumer::stream("persistent://*/article").unwrap();
        todo!()
    }

    #[tokio::test]
    async fn stream_use_ns_persistent() {
        let consumer = Consumer::stream("persistent://eu-west-1/article").unwrap();
        todo!()
    }
}
