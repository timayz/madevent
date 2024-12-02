use serde::{Deserialize, Serialize};
use sqlx::{FromRow, QueryBuilder, SqlitePool};
use std::any::type_name;
use thiserror::Error;
use ulid::Ulid;

pub struct Sender {
    pool: SqlitePool,
    aggregate: String,
    original_version: u16,
    events: Vec<(String, String, Vec<u8>, Option<Vec<u8>>)>,
}

impl Sender {
    pub fn new(aggregate: impl Into<String>, pool: &SqlitePool) -> Self {
        let aggregate = aggregate.into();

        Self {
            pool: pool.clone(),
            aggregate,
            events: vec![],
            original_version: 0,
        }
    }

    pub fn original_version(mut self, original_version: u16) -> Self {
        self.original_version = original_version;

        self
    }

    pub fn event<D>(
        self,
        data: &D,
    ) -> std::result::Result<Self, ciborium::ser::Error<std::io::Error>>
    where
        D: ?Sized + Serialize,
    {
        self.event_with_metadata_opt(data, None::<bool>.as_ref())
    }

    pub fn event_with_metadata<D, M>(
        self,
        data: &D,
        metadata: &M,
    ) -> std::result::Result<Self, ciborium::ser::Error<std::io::Error>>
    where
        D: ?Sized + Serialize,
        M: ?Sized + Serialize,
    {
        self.event_with_metadata_opt(data, Some(metadata))
    }

    fn event_with_metadata_opt<D, M>(
        mut self,
        data: &D,
        metadata: Option<&M>,
    ) -> std::result::Result<Self, ciborium::ser::Error<std::io::Error>>
    where
        D: ?Sized + Serialize,
        M: ?Sized + Serialize,
    {
        let id = Ulid::new().to_string();
        let name = type_name::<D>().to_owned();
        let mut data_encoded = Vec::new();
        ciborium::into_writer(data, &mut data_encoded)?;
        let metadata_encoded = if let Some(metadata) = metadata {
            let mut metadata_encoded = Vec::new();
            ciborium::into_writer(metadata, &mut metadata_encoded)?;
            Some(metadata_encoded)
        } else {
            None
        };

        self.events.push((id, name, data_encoded, metadata_encoded));

        Ok(self)
    }

    pub async fn send(&self) -> Result<()> {
        let mut version = self.original_version.to_owned();
        let mut tx = self.pool.begin().await?;

        let mut qb =
            QueryBuilder::new("INSERT INTO event (id, name, aggregate, version, data, metadata) ");

        qb.push_values(&self.events, |mut b, (id, name, data, metadata)| {
            version += 1;

            b.push_bind(id)
                .push_bind(name)
                .push_bind(self.aggregate.to_owned())
                .push_bind(version)
                .push_bind(data)
                .push_bind(metadata);
        });

        qb.push("ON CONFLICT (aggregate, version) DO NOTHING")
            .build()
            .execute(&mut *tx)
            .await
            .unwrap();

        let next = sqlx::query_as::<_, Event>(
            r#"
            SELECT * FROM event
            WHERE aggregate = ? AND version = ?
            ORDER BY timestamp ASC
            LIMIT 1
        "#,
        )
        .bind(&self.aggregate)
        .bind(i32::from(self.original_version + 1))
        .fetch_optional(&mut *tx)
        .await?;

        let invalid = if let (Some(next), Some(current)) = (next, self.events.first()) {
            next.id != current.0
        } else {
            false
        };

        if invalid {
            tx.rollback().await?;

            return Err(SenderError::InvalidOriginalVersion);
        }

        tx.commit().await?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SenderError {
    #[error("invalid original version")]
    InvalidOriginalVersion,

    #[error(transparent)]
    Ciborium(#[from] ciborium::ser::Error<String>),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

pub type Result<E> = std::result::Result<E, SenderError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub struct Event {
    pub id: String,
    pub name: String,
    pub aggregate: String,
    pub version: u16,
    pub data: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub timestamp: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use sqlx::{any::install_default_drivers, migrate::MigrateDatabase, Any};

    #[tokio::test]
    async fn send() {
        let pool = get_pool("sender_send").await;
        let mut fns = vec![];
        for key in 0..100 {
            let pool = pool.clone();
            fns.push(async move {
                let _ = Sender::new("product/1", &pool)
                    .event(&Created {
                        name: format!("Product {key}"),
                    }).unwrap()
                    .send()
                    .await;

                let _ = Sender::new("product/1", &pool)
                    .original_version(1)
                    .event_with_metadata(&VisibilityChanged { visible: false }, &Metadata { key }).unwrap()
                    .event(&ThumbnailChanged {
                        thumbnail: format!("product_{key}.png"),
                    }).unwrap()
                    .send()
                    .await;

                let _ = Sender::new("product/1", &pool)
                    .original_version(3)
                    .event(&Edited {
                        name: format!("Kit Ring Alarm XL {key}"),
                        description:
                            "Connected wireless home alarm, security system with assisted monitoring"
                        .to_owned(),
                        category: "ring".to_owned(),
                        visible: true,
                        stock: 100,
                        price: 309.99,
                    }).unwrap()
                    .send()
                    .await;

                let _ = Sender::new("product/1", &pool)
                    .original_version(4)
                    .event_with_metadata(&Deleted { deleted: true }, &Metadata { key }).unwrap()
                    .send()
                    .await;
            });
        }

        join_all(fns).await;

        let events = sqlx::query_as::<_, Event>(
            r#"
                SELECT * FROM event
                ORDER BY timestamp, version, id
            "#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(events.len(), 5);

        let event_1 = events.get(1).unwrap().clone();
        let metadata: Metadata = ciborium::from_reader(&event_1.metadata.unwrap()[..]).unwrap();

        assert_eq!(
            ciborium::from_reader::<Created, _>(&events[0].data[..]).unwrap(),
            Created {
                name: format!("Product {}", metadata.key)
            }
        );

        assert_eq!(
            ciborium::from_reader::<ThumbnailChanged, _>(&events[2].data[..]).unwrap(),
            ThumbnailChanged {
                thumbnail: format!("product_{}.png", metadata.key),
            }
        );

        assert_eq!(
            ciborium::from_reader::<Edited, _>(&events[3].data[..]).unwrap(),
            Edited {
                name: format!("Kit Ring Alarm XL {}", metadata.key),
                description:
                    "Connected wireless home alarm, security system with assisted monitoring"
                        .to_owned(),
                category: "ring".to_owned(),
                visible: true,
                stock: 100,
                price: 309.99,
            }
        );

        assert_eq!(
            ciborium::from_reader::<Metadata, _>(&events[4].metadata.clone().unwrap()[..]).unwrap(),
            Metadata { key: metadata.key }
        );
    }

    #[tokio::test]
    async fn invalid_original_version() {
        let pool = get_pool("sender_invalid_original_version").await;

        let res = Sender::new("product/1", &pool)
            .event(&Created {
                name: "Product 1".to_owned(),
            })
            .unwrap()
            .send()
            .await;

        assert!(res.is_ok());

        let err = Sender::new("product/1", &pool)
            .event(&VisibilityChanged { visible: false })
            .unwrap()
            .send()
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            SenderError::InvalidOriginalVersion.to_string()
        );

        let res = Sender::new("product/1", &pool)
            .original_version(1)
            .event(&Deleted { deleted: true })
            .unwrap()
            .send()
            .await;

        assert!(res.is_ok());
    }

    async fn get_pool(key: impl Into<String>) -> SqlitePool {
        let key = key.into();
        let dsn = format!("sqlite:../target/{key}.db");

        install_default_drivers();
        let _ = Any::drop_database(&dsn).await;
        Any::create_database(&dsn).await.unwrap();

        let pool = SqlitePool::connect(&dsn).await.unwrap();
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        pool
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Created {
        pub name: String,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Deleted {
        pub deleted: bool,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Edited {
        pub name: String,
        pub description: String,
        pub category: String,
        pub visible: bool,
        pub stock: i32,
        pub price: f32,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct VisibilityChanged {
        pub visible: bool,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct ThumbnailChanged {
        pub thumbnail: String,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Metadata {
        pub key: i32,
    }
}
