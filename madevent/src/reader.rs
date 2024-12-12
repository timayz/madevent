use crate::Event;
use futures::{stream, Stream};

pub struct Reader {
    key: String,
    use_history: bool,
    use_key: bool,
}

impl Reader {
    pub fn new(key: impl Into<String>) -> Self {
        let key = key.into();
        Self {
            key,
            use_history: true,
            use_key: true,
        }
    }

    pub fn read(&self, aggregate: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }

    pub fn stream(&self, filter: impl Into<String>) -> impl Stream<Item = Event> {
        stream::iter(vec![])
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn read() {
        todo!()
    }

    #[tokio::test]
    async fn stream() {
        todo!()
    }

    #[tokio::test]
    async fn stream_with_cursor() {
        todo!()
    }

    #[tokio::test]
    async fn stream_no_key() {
        todo!()
    }

    #[tokio::test]
    async fn stram_no_key_with_cursor() {
        todo!()
    }
}
