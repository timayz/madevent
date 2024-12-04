use serde::Serialize;
use sqlx::{QueryBuilder, SqlitePool};
use std::any::type_name;
use thiserror::Error;
use ulid::Ulid;

pub struct Writer {
    pool: SqlitePool,
    aggregate: String,
    original_version: u16,
    events: Vec<(String, Vec<u8>, Option<Vec<u8>>)>,
}

impl Writer {
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

        self.events.push((name, data_encoded, metadata_encoded));

        Ok(self)
    }

    pub async fn write(&self) -> Result<()> {
        let mut version = self.original_version.to_owned();
        let mut tx = self.pool.begin().await?;

        let mut qb =
            QueryBuilder::new("INSERT INTO event (id, name, aggregate, version, data, metadata) ");

        qb.push_values(&self.events, |mut b, (name, data, metadata)| {
            version += 1;

            let id = Ulid::new().to_string();
            b.push_bind(id)
                .push_bind(name)
                .push_bind(self.aggregate.to_owned())
                .push_bind(version)
                .push_bind(data)
                .push_bind(metadata);
        });

        let Err(e) = qb.build().execute(&mut *tx).await else {
            tx.commit().await?;

            return Ok(());
        };

        if e.to_string().contains("(code: 2067)") {
            Err(WriterError::InvalidOriginalVersion)
        } else {
            Err(e.into())
        }
    }
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("invalid original version")]
    InvalidOriginalVersion,

    #[error(transparent)]
    Ciborium(#[from] ciborium::ser::Error<String>),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

pub type Result<E> = std::result::Result<E, WriterError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use futures::future::join_all;
    use serde::Deserialize;
    use sqlx::{any::install_default_drivers, migrate::MigrateDatabase, Any};

    #[tokio::test]
    async fn send() {
        let pool = get_pool("sender_send").await;
        let mut fns = vec![];
        for _ in 0..100 {
            let pool = pool.clone();
            fns.push(async move {
                let _ = Writer::new("product/1", &pool)
                    .event(&Created {
                        name: format!("Product 1"),
                    }).unwrap()
                    .write()
                    .await;

                let _ = Writer::new("product/1", &pool)
                    .original_version(1)
                    .event_with_metadata(&VisibilityChanged { visible: false }, &Metadata { key: 23 }).unwrap()
                    .event(&ThumbnailChanged {
                        thumbnail: format!("product_1.png"),
                    }).unwrap()
                    .write()
                    .await;

                let _ = Writer::new("product/1", &pool)
                    .original_version(3)
                    .event(&Edited {
                        name: format!("Kit Ring Alarm XL"),
                        description:
                            "Connected wireless home alarm, security system with assisted monitoring"
                        .to_owned(),
                        category: "ring".to_owned(),
                        visible: true,
                        stock: 100,
                        price: 309.99,
                    }).unwrap()
                    .write()
                    .await;

                let _ = Writer::new("product/1", &pool)
                    .original_version(4)
                    .event_with_metadata(&Deleted { deleted: true }, &Metadata { key: 34 }).unwrap()
                    .write()
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

        assert_eq!(
            events[0].to_data::<Created>().unwrap().unwrap(),
            Created {
                name: "Product 1".to_owned(),
            }
        );

        assert_eq!(
            events[1].to_metadata::<Metadata>().unwrap().unwrap(),
            Metadata { key: 23 }
        );

        assert_eq!(
            events[1].to_data::<VisibilityChanged>().unwrap().unwrap(),
            VisibilityChanged { visible: false }
        );

        assert_eq!(
            events[2].to_data::<ThumbnailChanged>().unwrap().unwrap(),
            ThumbnailChanged {
                thumbnail: "product_1.png".to_owned(),
            }
        );

        assert_eq!(
            events[3].to_data::<Edited>().unwrap().unwrap(),
            Edited {
                name: "Kit Ring Alarm XL".to_owned(),
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
            events[4].to_metadata::<Metadata>().unwrap().unwrap(),
            Metadata { key: 34 }
        );

        assert_eq!(
            events[4].to_data::<Deleted>().unwrap().unwrap(),
            Deleted { deleted: true }
        );
    }

    #[tokio::test]
    async fn invalid_original_version() {
        let pool = get_pool("sender_invalid_original_version").await;

        let res = Writer::new("product/1", &pool)
            .event(&Created {
                name: "Product 1".to_owned(),
            })
            .unwrap()
            .write()
            .await;

        assert!(res.is_ok());

        let err = Writer::new("product/1", &pool)
            .event(&VisibilityChanged { visible: false })
            .unwrap()
            .write()
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            WriterError::InvalidOriginalVersion.to_string()
        );

        let res = Writer::new("product/1", &pool)
            .original_version(1)
            .event(&Deleted { deleted: true })
            .unwrap()
            .write()
            .await;

        assert!(res.is_ok());
    }

    async fn get_pool(key: impl Into<String>) -> SqlitePool {
        let key = key.into();
        let dsn = format!("sqlite:../target/writer_{key}.db");

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
