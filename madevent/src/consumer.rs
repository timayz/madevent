use crate::{Cursor, Event, Query};
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("url: {0}")]
    Url(#[from] url::ParseError),

    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("cursor: {0}")]
    Cursor(#[from] crate::cursor::Error),

    #[error("bad scheme: must be persistent or non-persistent")]
    BadScheme,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub struct Consumer {
    pub id: String,
    pub cursor: String,
}

impl Consumer {
    pub async fn stream(
        id: impl Into<String>,
        filter: impl Into<String>,
        executor: &SqlitePool,
    ) -> Result<impl Stream<Item = Event>, ConsumerError> {
        let url = Url::parse(&filter.into())?;
        let id = id.into();
        let cursor = match url.scheme() {
            "persistent" => {
                sqlx::query_as::<_, (String,)>("SELECT cursor FROM consumer WHERE id = ? LIMIT 1")
                    .bind(id)
                    .fetch_optional(executor)
                    .await?
                    .map(|c| Cursor(c.0))
            }
            "non-persistent" => Query::<_, Event>::new("SELECT * FROM event")
                .backward(1, None)
                .query(executor)
                .await?
                .edges
                .first()
                .map(|e| e.cursor.clone()),
            _ => return Err(ConsumerError::BadScheme),
        };
        let filter = format!("{}{}", url.host_str().unwrap_or_default(), url.path());
        Ok(stream::unfold((filter, cursor, executor.clone()), {
            |(filter, cursor, executor)| async move {
                let mut interval = tokio::time::interval_at(
                    tokio::time::Instant::now(),
                    tokio::time::Duration::from_millis(150),
                );
                loop {

                    let Ok(res) = Query::<_, Event>::new("SELECT * FROM event")
                        .forward(1, cursor.clone())
                        .query(&executor.clone())
                        .await
                    else {
                        return None;
                    };

                    if let Some(edge) = res.edges.first() {
                        return Some((
                            edge.node.clone(),
                            (filter, Some(edge.cursor.clone()), executor),
                        ));
                    }

                    interval.tick().await;
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, Producer, Query};
    use fake::{
        faker::{
            internet::en::{SafeEmail, Username},
            name::en::Name,
        },
        Dummy, Fake, Faker,
    };
    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use sqlx::{
        any::{install_default_drivers, Any},
        migrate::MigrateDatabase,
        SqlitePool,
    };
    use std::collections::HashMap;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn stream_all_non_persistent() {
        let pool = init_data("stream_all_non_persistent").await;
        let (_, b, c) = generate_events(&pool, "non-persistent://*/user").await;
assert_eq!(b.len(), c.len());
        assert_eq!(b, c);
    }

    #[tokio::test]
    async fn stream_non_persistent() {
        let pool = init_data("stream_non_persistent").await;
        let tenant = get_tenant();
        let (_, b, c) = generate_events(&pool, format!("non-persistent://{tenant}/user")).await;

        assert_eq!(
            get_tenant_events(&tenant, &b),
            get_tenant_events(&tenant, &c)
        );
    }

    #[tokio::test]
    async fn stream_all_persistent() {
        let pool = init_data("stream_all_persistent").await;
        let (mut a, b, c) = generate_events(&pool, "persistent://*/user").await;
        a.extend(b);

        assert_eq!(a, c)
    }

    #[tokio::test]
    async fn stream_persistent() {
        let pool = init_data("stream_persistent").await;
        let tenant = get_tenant();
        let (mut a, b, c) = generate_events(&pool, format!("persistent://{tenant}/user")).await;

        a.extend(b);

        assert_eq!(
            get_tenant_events(&tenant, &a),
            get_tenant_events(&tenant, &c)
        )
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

    async fn get_events(pool: &SqlitePool, event_version: &mut HashMap<u8, u16>) -> Vec<Event> {
        let cursor = Query::<_, Event>::new("SELECT * FROM event")
            .backward(1, None)
            .query(pool)
            .await
            .unwrap()
            .edges
            .first()
            .map(|e| e.cursor.clone());

        for _ in 0..100 {
            let user: User = Faker.fake();
            let group: User = Faker.fake();
            let version = event_version.entry(user.id).or_default();
            let producer = Producer::new(user.id.to_string())
                .tenant(format!("group-{}", group.id))
                .topic("user")
                .original_version(version.to_owned());
            let writer = match user.evt_rand {
                0 => producer.event::<UsermameChanged>(&Faker.fake()),
                1 => producer.event::<DisplayNameChanged>(&Faker.fake()),
                2 => producer.event::<EmailChanged>(&Faker.fake()),
                _ => unreachable!(),
            };
            writer.unwrap().publish(pool).await.unwrap();
            *version += 1;
        }

        Query::<_, Event>::new("SELECT * FROM event")
            .forward(1000, cursor)
            .query(pool)
            .await
            .map(|r| r.edges.into_iter().map(|e| e.node).collect())
            .unwrap()
    }

    async fn generate_events(
        pool: &SqlitePool,
        filter: impl Into<String>,
    ) -> (Vec<Event>, Vec<Event>, Vec<Event>) {
        let filter = filter.into();
        let mut event_version = HashMap::new();
        let a = get_events(&pool, &mut event_version).await;

        let c_pool = pool.clone();
        let c = tokio::spawn(async move {
            let consumer = Consumer::stream("consumer-1", filter, &c_pool)
                .await
                .unwrap();
            let mut events = vec![];

            tokio::pin!(consumer);
            while let Ok(Some(event)) = timeout(Duration::from_secs(2), consumer.next()).await {
                events.push(event);
            }

            events
        })
        .await
        .unwrap();

        let b = get_events(&pool, &mut event_version).await;

        (a, b, c)
    }

    fn get_tenant() -> String {
        let group: User = Faker.fake();
        format!("group-{}", group.id)
    }

    fn get_tenant_events(tenant: impl Into<String>, events: &Vec<Event>) -> Vec<Event> {
        let tenant = tenant.into();
        events
            .clone()
            .into_iter()
            .filter(|e| e.tenant.as_ref() == Some(&tenant))
            .collect()
    }
}
