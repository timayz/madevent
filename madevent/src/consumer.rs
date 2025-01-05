use crate::{cursor::Edge, Cursor, Event, Query, ToCursor};
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use std::collections::HashMap;
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

struct ConsumerStreamContext {
    id: String,
    worker_id: Option<String>,
    tenant: Option<String>,
    topic: String,
    executor: SqlitePool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub struct Consumer {
    pub id: String,
    pub cursor: String,
}

impl Consumer {
    pub async fn stream(
        id: impl Into<String>,
        url: impl Into<String>,
        executor: &SqlitePool,
    ) -> Result<impl Stream<Item = Edge<Event>>, ConsumerError> {
        let url = Url::parse(&url.into())?;
        let id = id.into();
        let (worker_id, cursor) = match url.scheme() {
            "persistent" => {
                let worker_id = ulid::Ulid::new().to_string();

                sqlx::query(
                    r#"
                    INSERT INTO consumer(id,worker_id) VALUES (?,?)
                    ON CONFLICT(id) DO UPDATE SET worker_id=excluded.worker_id
                    "#,
                )
                .bind(&id)
                .bind(&worker_id)
                .execute(executor)
                .await?;

                let cursor = sqlx::query_as::<_, (Option<String>,)>(
                    "SELECT cursor FROM consumer WHERE id = ? AND worker_id = ? LIMIT 1",
                )
                .bind(&id)
                .bind(&worker_id)
                .fetch_one(executor)
                .await?;

                (Some(worker_id), cursor.0.map(|v| Cursor(v)))
            }
            "non-persistent" => {
                let cursor = Query::<_, Event>::new("SELECT * FROM event")
                    .backward(1, None)
                    .query(executor)
                    .await?
                    .edges
                    .first()
                    .map(|e| e.cursor.clone());
                (None, cursor)
            }
            _ => return Err(ConsumerError::BadScheme),
        };

        let topic = format!("{}{}", url.host_str().unwrap_or_default(), url.path());
        let query_params = url.query_pairs().into_owned().collect::<HashMap<_, _>>();
        let tenant = query_params.get("tenant").map(|t| t.to_string());

        Ok(stream::unfold(
            (
                ConsumerStreamContext {
                    tenant,
                    worker_id,
                    id,
                    topic,
                    executor: executor.clone(),
                },
                cursor,
            ),
            {
                |(ctx, cursor)| async move {
                    let mut interval = tokio::time::interval_at(
                        tokio::time::Instant::now(),
                        tokio::time::Duration::from_millis(150),
                    );

                    loop {
                        if let Some(worker_id) = &ctx.worker_id {
                            let Ok(res) = sqlx::query_as::<_, (String,)>(
                                "SELECT id FROM consumer WHERE id = ? AND worker_id = ? LIMIT 1",
                            )
                            .bind(&ctx.id)
                            .bind(worker_id)
                            .fetch_optional(&ctx.executor)
                            .await
                            else {
                                // @TODO: LOG ME
                                return None;
                            };

                            if res.is_none() {
                                return None;
                            }
                        }

                        let query = if let Some(tenant) = ctx.tenant.to_owned() {
                            Query::<_, Event>::new(
                                "SELECT * FROM event WHERE tenant = ? AND topic = ?",
                            )
                            .bind(tenant)
                            .expect("failed to bind tenant")
                            .bind(ctx.topic.to_owned())
                            .expect("failed to bind topic")
                        } else {
                            Query::<_, Event>::new("SELECT * FROM event WHERE topic = ?")
                                .bind(ctx.topic.to_owned())
                                .expect("failed to bind topic")
                        };

                        let Ok(res) = query.forward(1, cursor.clone()).query(&ctx.executor).await
                        else {
                            // @TODO: LOG ME
                            return None;
                        };

                        if let Some(edge) = res.edges.first() {
                            return Some((edge.clone(), (ctx, Some(edge.cursor.clone()))));
                        }

                        interval.tick().await;
                    }
                }
            },
        ))
    }

    pub async fn ack(
        id: impl Into<String>,
        cursor: impl Into<String>,
        executor: &SqlitePool,
    ) -> Result<(), ConsumerError> {
        let id = id.into();
        let cursor = cursor.into();

        sqlx::query("UPDATE consumer SET cursor = ?, updated_at = datetime('now') WHERE id = ?")
            .bind(cursor)
            .bind(id)
            .execute(executor)
            .await?;

        Ok(())
    }

    pub async fn unack(
        id: impl Into<String>,
        event_id: impl Into<String>,
        reason: impl Into<String>,
        executor: &SqlitePool,
    ) -> Result<(), ConsumerError> {
        todo!()
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

    #[tokio::test]
    async fn stream_all_non_persistent() {
        let pool = init_data("stream_all_non_persistent").await;
        let mut versions = HashMap::new();

        get_events(&pool, &mut versions).await;

        let consumer_1 = Consumer::stream("consumer-1", "non-persistent://user", &pool)
            .await
            .unwrap();

        let consumer_2 = Consumer::stream("consumer-2", "non-persistent://user", &pool)
            .await
            .unwrap();

        tokio::pin!(consumer_1);
        tokio::pin!(consumer_2);

        let events = get_events(&pool, &mut versions).await;
        for event in events {
            assert_eq!(Some(&event), consumer_1.next().await.as_ref());
            assert_eq!(Some(&event), consumer_2.next().await.as_ref());
        }
    }

    #[tokio::test]
    async fn stream_non_persistent() {
        let pool = init_data("stream_non_persistent").await;
        let mut versions = HashMap::new();
        let tenant = get_tenant();

        get_events(&pool, &mut versions).await;

        let consumer_1 = Consumer::stream(
            "consumer-1",
            format!("non-persistent://user?tenant={tenant}"),
            &pool,
        )
        .await
        .unwrap();

        tokio::pin!(consumer_1);

        let consumer_2 = Consumer::stream(
            "consumer-2",
            format!("non-persistent://user?tenant={tenant}"),
            &pool,
        )
        .await
        .unwrap();

        tokio::pin!(consumer_2);

        let events = get_events(&pool, &mut versions).await;
        let events = get_tenant_events(&tenant, &events);

        for event in events {
            assert_eq!(Some(&event), consumer_1.next().await.as_ref());
            assert_eq!(Some(&event), consumer_2.next().await.as_ref());
        }
    }

    #[tokio::test]
    async fn stream_all_persistent() {
        let pool = init_data("stream_all_persistent").await;
        let mut versions = HashMap::new();
        get_events(&pool, &mut versions).await;

        let consumer_1 = Consumer::stream("consumer-1", "persistent://user", &pool)
            .await
            .unwrap();

        tokio::pin!(consumer_1);

        let consumer_2 = Consumer::stream("consumer-2", "persistent://user", &pool)
            .await
            .unwrap();

        tokio::pin!(consumer_2);

        get_events(&pool, &mut versions).await;

        let events = Query::<_, Event>::new("SELECT * FROM event")
            .forward(1000, None)
            .query(&pool)
            .await
            .unwrap()
            .edges;

        for event in events {
            assert_eq!(Some(&event), consumer_1.next().await.as_ref());
            assert_eq!(Some(&event), consumer_2.next().await.as_ref());

            Consumer::ack("consumer-1", &event.cursor.0, &pool)
                .await
                .unwrap();

            Consumer::ack("consumer-2", &event.cursor.0, &pool)
                .await
                .unwrap();
        }

        drop(consumer_1);
        drop(consumer_2);

        let consumer_1 = Consumer::stream("consumer-1", "persistent://user", &pool)
            .await
            .unwrap();

        tokio::pin!(consumer_1);

        let consumer_2 = Consumer::stream("consumer-2", "persistent://user", &pool)
            .await
            .unwrap();

        tokio::pin!(consumer_2);

        let events = get_events(&pool, &mut versions).await;
        for event in events {
            assert_eq!(Some(&event), consumer_1.next().await.as_ref());
            assert_eq!(Some(&event), consumer_2.next().await.as_ref());
        }
    }

    #[tokio::test]
    async fn stream_persistent() {
        let pool = init_data("stream_persistent").await;
        let mut versions = HashMap::new();

        get_events(&pool, &mut versions).await;

        let tenant = get_tenant();

        let consumer_1 = Consumer::stream(
            "consumer-1",
            format!("persistent://user?tenant={tenant}"),
            &pool,
        )
        .await
        .unwrap();

        tokio::pin!(consumer_1);

        let consumer_2 = Consumer::stream(
            "consumer-2",
            format!("persistent://user?tenant={tenant}"),
            &pool,
        )
        .await
        .unwrap();

        tokio::pin!(consumer_2);

        get_events(&pool, &mut versions).await;

        let events = Query::<_, Event>::new("SELECT * FROM event")
            .forward(1000, None)
            .query(&pool)
            .await
            .unwrap()
            .edges;

        let events = get_tenant_events(&tenant, &events);

        for event in events {
            assert_eq!(Some(&event), consumer_1.next().await.as_ref());
            assert_eq!(Some(&event), consumer_2.next().await.as_ref());

            Consumer::ack("consumer-1", &event.cursor.0, &pool)
                .await
                .unwrap();

            Consumer::ack("consumer-2", &event.cursor.0, &pool)
                .await
                .unwrap();
        }

        drop(consumer_1);
        drop(consumer_2);

        let consumer_1 = Consumer::stream(
            "consumer-1",
            format!("persistent://user?tenant={tenant}"),
            &pool,
        )
        .await
        .unwrap();

        tokio::pin!(consumer_1);

        let consumer_2 = Consumer::stream(
            "consumer-2",
            format!("persistent://user?tenant={tenant}"),
            &pool,
        )
        .await
        .unwrap();

        tokio::pin!(consumer_2);

        let events = get_events(&pool, &mut versions).await;
        let events = get_tenant_events(&tenant, &events);

        for event in events {
            // @TODO: fix random failed
            assert_eq!(Some(&event), consumer_1.next().await.as_ref());
            assert_eq!(Some(&event), consumer_2.next().await.as_ref());
        }
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

    async fn get_events(
        pool: &SqlitePool,
        event_version: &mut HashMap<u8, u16>,
    ) -> Vec<Edge<Event>> {
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
            .map(|r| r.edges.clone())
            .unwrap()
    }

    fn get_tenant() -> String {
        let group: User = Faker.fake();
        format!("group-{}", group.id)
    }

    fn get_tenant_events(tenant: impl Into<String>, events: &Vec<Edge<Event>>) -> Vec<Edge<Event>> {
        let tenant = tenant.into();
        events
            .clone()
            .into_iter()
            .filter(|e| e.node.tenant.as_ref() == Some(&tenant))
            .collect()
    }
}
