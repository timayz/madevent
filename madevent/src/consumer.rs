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
    use crate::{Event, Producer};
    use fake::{
        faker::{
            internet::en::{SafeEmail, Username},
            name::en::Name,
        },
        Dummy, Fake, Faker,
    };
    use rand::{seq::SliceRandom, Rng};
    use serde::{Deserialize, Serialize};
    use sqlx::{
        any::{install_default_drivers, Any},
        migrate::MigrateDatabase,
        SqlitePool,
    };
    use std::collections::HashMap;

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

    #[derive(Debug, PartialEq, Deserialize, Serialize, Dummy)]
    struct UsermameChanged {
        #[dummy(faker = "Username()")]
        pub username: String,
    }

    #[derive(Debug, PartialEq, Deserialize, Serialize, Dummy)]
    struct DisplayNameChanged {
        #[dummy(faker = "Name()")]
        pub display_name: String,
    }

    #[derive(Debug, PartialEq, Deserialize, Serialize, Dummy)]
    struct EmailChanged {
        #[dummy(faker = "SafeEmail()")]
        pub email: String,
    }

    #[derive(Debug, Dummy)]
    struct User {
        #[dummy(faker = "0..10")]
        pub id: u8,

        #[dummy(faker = "0..2")]
        pub evt_rand: u8,
    }

    async fn init_data(key: impl Into<String>) -> SqlitePool {
        let key = key.into();
        let dsn = format!("sqlite:../target/consumer_{key}.db");

        install_default_drivers();
        let _ = Any::drop_database(&dsn).await;
        Any::create_database(&dsn).await.unwrap();

        let pool = SqlitePool::connect(&dsn).await.unwrap();
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        pool
    }

    async fn get_events(pool: &SqlitePool) -> Vec<Event> {
        let mut event_version: HashMap<u8, u16> = HashMap::new();

        for _ in 0..100 {
            let user: User = Faker.fake();
            let group: User = Faker.fake();
            let version = event_version.entry(user.id).or_default();
            let producer =
                Producer::new(format!("group-{}/user", group.id), user.id.to_string()).original_version(version.to_owned());
            let writer = match user.evt_rand {
                0 => producer.event::<UsermameChanged>(&Faker.fake()),
                1 => producer.event::<DisplayNameChanged>(&Faker.fake()),
                2 => producer.event::<EmailChanged>(&Faker.fake()),
                _ => unreachable!(),
            };
            writer.unwrap().publish(pool).await.unwrap();
            *version += 1;
        }

        sqlx::query_as::<_, Event>(&format!(
            "select * from event order by timestamp ASC, version ASC, id ASC"
        ))
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn get_topic_events(events: &Vec<Event>) -> (String, Vec<Event>) {
        let group: User = Faker.fake();
        let events = events
            .clone()
            .into_iter()
            .filter(|e| e.topic == group.id.to_string())
            .collect();

        (group.id.to_string(), events)
    }
}
