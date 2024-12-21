use crate::Event;
use futures::{stream, Stream};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("url: {0}")]
    Url(#[from] url::ParseError),

    #[error("bad scheme: must be persistent or non-persistent")]
    BadScheme,
}

pub struct Consumer {
    path: String,
    persistent: bool,
}

impl Consumer {
    pub fn stream(filter: impl Into<String>) -> Result<impl Stream<Item = Event>, ConsumerError> {
        let url = Url::parse(&filter.into())?;
        let persistent = match url.scheme() {
            "persistent" => true,
            "non-persistent" => false,
            _ => return Err(ConsumerError::BadScheme),
        };
        let filter = format!("{}{}", url.host_str().unwrap_or_default(), url.path());
        Ok(stream::iter(vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stream_all_non_persistent() {
        let consumer = Consumer::stream("non-persistent://*/article").unwrap();
        todo!()
    }

    #[tokio::test]
    async fn stream_non_persistent() {
        let consumer = Consumer::stream("non-persistent://eu-west-1/article").unwrap();
        todo!()
    }

    #[tokio::test]
    async fn stream_all_persistent() {
        let consumer = Consumer::stream("persistent://*/article").unwrap();
        todo!()
    }

    #[tokio::test]
    async fn stream_persistent() {
        let consumer = Consumer::stream("persistent://eu-west-1/article").unwrap();
        todo!()
    }
}
