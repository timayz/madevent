use serde::{Deserialize, Serialize};
use sqlx::{Arguments, Database, Encode, Executor, FromRow, IntoArguments, QueryBuilder, Type};
use std::marker::PhantomData;

pub type SqliteReader<'args, O> =
    Reader<'args, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'args>, O>;

pub struct Reader<'args, DB, A, O>
where
    DB: Database,
    A: Arguments<'args, Database = DB> + IntoArguments<'args, DB> + Clone,
    O: for<'r> FromRow<'r, DB::Row>,
    O: 'args + Send + Unpin,
    O: 'args + FromCursor,
{
    qb: QueryBuilder<'args, DB>,
    args: A,
    inner: PhantomData<O>,
}

impl<'args, DB, A, O> Reader<'args, DB, A, O>
where
    DB: Database,
    A: Arguments<'args, Database = DB> + IntoArguments<'args, DB> + Clone,
    O: for<'r> FromRow<'r, DB::Row>,
    O: 'args + Send + Unpin,
    O: 'args + FromCursor,
{
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            qb: QueryBuilder::new(sql),
            args: A::default(),
            inner: PhantomData,
        }
    }

    pub fn bind<Arg>(mut self, arg: Arg) -> Result<Self, sqlx::error::BoxDynError>
    where
        Arg: 'args + Send + Encode<'args, DB> + Type<DB>,
    {
        self.args.add(arg)?;
        Ok(self)
    }

    pub fn order(mut self, value: Order) -> Self {
        todo!()
    }

    pub fn args(mut self, value: Args) -> Self {
        todo!()
    }

    pub fn desc(mut self) -> Self {
        self.order(Order::Desc)
    }

    pub fn backward(mut self, last: u16, before: Option<Cursor>) -> Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }
    pub fn forward(mut self, first: u16, after: Option<Cursor>) -> Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

    pub async fn read<E>(&'args self, _executor: E) -> ReadResult<O>
    where
        E: 'args + Executor<'args, Database = DB>,
    {
        //let mut query = sqlx::query_as_with::<_, O, _>(self.qb.sql(), self.args.clone());
        //let mut rows = query.fetch_all(executor).await.unwrap();
        todo!()
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Cursor(pub String);

impl From<String> for Cursor {
    fn from(val: String) -> Self {
        Self(val)
    }
}

impl AsRef<[u8]> for Cursor {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub trait ToCursor {
    fn to_cursor(&self) -> Cursor;
}

pub trait FromCursor {
    fn from_cursor<A>(value: &Cursor) -> A;
}

#[derive(Debug, Clone)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Edge<N> {
    pub cursor: Cursor,
    pub node: N,
}

impl<N: ToCursor> From<N> for Edge<N> {
    fn from(value: N) -> Self {
        Self {
            cursor: value.to_cursor(),
            node: value,
        }
    }
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PageInfo {
    pub has_previous_page: bool,
    pub has_next_page: bool,
    pub start_cursor: Option<Cursor>,
    pub end_cursor: Option<Cursor>,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadResult<N> {
    pub edges: Vec<Edge<N>>,
    pub page_info: PageInfo,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Args {
    pub first: Option<u16>,
    pub after: Option<Cursor>,
    pub last: Option<u16>,
    pub before: Option<Cursor>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use crate::Writer;
    use fake::{
        faker::{
            internet::en::{SafeEmail, Username},
            name::en::Name,
        },
        Dummy, Fake, Faker,
    };
    use serde::{Deserialize, Serialize};
    use sqlx::{
        any::{install_default_drivers, Any},
        migrate::MigrateDatabase,
        SqlitePool,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn forward() {
        let pool = init_data("forward").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn forward_desc() {
        let pool = init_data("forward_desc").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn backward() {
        let pool = init_data("backward").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn backward_desc() {
        let pool = init_data("backward_desc").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn aggregate_forward() {
        let pool = init_data("aggregate_forward").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn aggregate_forward_desc() {
        let pool = init_data("aggregate_forward_desc").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn aggregate_backward() {
        let pool = init_data("aggregate_backward").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    #[tokio::test]
    async fn aggregate_backward_desc() {
        let pool = init_data("aggregate_backward_desc").await;
        let (events, user_events) = get_events(&pool).await;
        let r_events = all_reader().read(&pool).await;
        //sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
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

    fn all_reader<'a>() -> SqliteReader<'a, Event> {
        SqliteReader::new("select * from event")
    }

    fn aggregate_reader<'a>(value: impl Into<String>) -> SqliteReader<'a, Event> {
        let value = value.into();
        SqliteReader::new("SELECT * FROM event WHERE aggregate = ?")
            .bind(value)
            .unwrap()
    }

    async fn init_data(key: impl Into<String>) -> SqlitePool {
        let key = key.into();
        let dsn = format!("sqlite:../target/reader_{key}.db");

        install_default_drivers();
        let _ = Any::drop_database(&dsn).await;
        Any::create_database(&dsn).await.unwrap();

        let pool = SqlitePool::connect(&dsn).await.unwrap();
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        pool
    }

    async fn get_events(pool: &SqlitePool) -> (Vec<Event>, HashMap<String, Vec<Event>>) {
        let mut event_version: HashMap<u8, u16> = HashMap::new();

        for _ in 0..100 {
            let user: User = Faker.fake();
            let version = event_version.entry(user.id).or_default();
            let writer =
                Writer::new(format!("user/{}", user.id)).original_version(version.to_owned());
            let writer = match user.evt_rand {
                0 => writer.event::<UsermameChanged>(&Faker.fake()),
                1 => writer.event::<DisplayNameChanged>(&Faker.fake()),
                2 => writer.event::<EmailChanged>(&Faker.fake()),
                _ => unreachable!(),
            };
            writer.unwrap().write(pool).await.unwrap();
            *version += 1;
        }

        let events =
            sqlx::query_as::<_, Event>("select * from event order by timestamp, version, id")
                .fetch_all(pool)
                .await
                .unwrap();
        let mut user_events: HashMap<String, Vec<Event>> = HashMap::new();

        for event in &events {
            let user_event = user_events.entry(event.aggregate.to_owned()).or_default();
            user_event.push(event.clone());
        }

        (events, user_events)
    }
}
