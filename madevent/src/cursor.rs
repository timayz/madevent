use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{
    query::QueryAs, Arguments, Database, Encode, Executor, FromRow, IntoArguments, QueryBuilder,
    Type,
};
use std::marker::PhantomData;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cursor: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("cbor de")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("base64 decode: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("cbor de: {0}")]
    CiboriumDe(#[from] ciborium::de::Error<std::io::Error>),
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
    type Cursor: Serialize;

    fn serialize_cursor(&self) -> Self::Cursor;
    fn to_cursor(&self) -> Result<Cursor, ciborium::ser::Error<std::io::Error>> {
        let cursor = self.serialize_cursor();

        let mut cbor_encoded = vec![];
        ciborium::into_writer(&cursor, &mut cbor_encoded)?;

        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        Ok(Cursor(engine.encode(cbor_encoded)))
    }
}

pub trait BindCursor<'q, DB: Database> {
    type Cursor: DeserializeOwned;

    fn bing_keys() -> Vec<&'static str>;

    fn bind_query<O>(
        cursor: Self::Cursor,
        query: QueryAs<'q, DB, O, DB::Arguments<'q>>,
    ) -> QueryAs<'q, DB, O, DB::Arguments<'q>>;

    fn bind_cursor<O>(
        value: &Cursor,
        query: QueryAs<'q, DB, O, DB::Arguments<'q>>,
    ) -> Result<QueryAs<'q, DB, O, DB::Arguments<'q>>, Error> {
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(value)?;
        let cursor = ciborium::from_reader(&decoded[..])?;

        Ok(Self::bind_query(cursor, query))
    }
}

pub struct Query<'args, DB, O>
where
    DB: Database,
    DB::Arguments<'args>: IntoArguments<'args, DB> + Clone,
    O: for<'r> FromRow<'r, DB::Row>,
    O: 'args + Send + Unpin,
    O: 'args + BindCursor<'args, DB> + ToCursor,
{
    qb: QueryBuilder<'args, DB>,
    qb_args: DB::Arguments<'args>,
    phantom_o: PhantomData<O>,
    order: Order,
    args: Args,
}

impl<'args, DB, O> Query<'args, DB, O>
where
    DB: Database,
    DB::Arguments<'args>: IntoArguments<'args, DB> + Clone,
    O: for<'r> FromRow<'r, DB::Row>,
    O: 'args + Send + Unpin,
    O: 'args + BindCursor<'args, DB> + ToCursor,
{
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            qb: QueryBuilder::new(sql),
            qb_args: DB::Arguments::default(),
            phantom_o: PhantomData,
            order: Order::Asc,
            args: Default::default(),
        }
    }

    pub fn bind<Arg>(mut self, arg: Arg) -> Result<Self, sqlx::error::BoxDynError>
    where
        Arg: 'args + Send + Encode<'args, DB> + Type<DB>,
    {
        self.qb_args.add(arg)?;
        Ok(self)
    }

    pub fn order(mut self, value: Order) -> Self {
        self.order = value;

        self
    }

    pub fn args(mut self, value: Args) -> Self {
        self.args = value;

        self
    }

    pub fn desc(self) -> Self {
        self.order(Order::Desc)
    }

    pub fn backward(self, last: u16, before: Option<Cursor>) -> Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }

    pub fn forward(self, first: u16, after: Option<Cursor>) -> Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

    pub async fn query<'a, E>(&'args mut self, executor: E) -> Result<ReadResult<O>, Error>
    where
        E: 'a + Executor<'a, Database = DB>,
    {
        let (limit, cursor) = self.build();

        let mut query = sqlx::query_as_with::<_, O, _>(self.qb.sql(), self.qb_args.clone());
        if let Some(cursor) = cursor {
            query = O::bind_cursor(&cursor, query)?;
        }
        let mut rows = query.fetch_all(executor).await?;
        let has_more = rows.len() > limit as usize;

        if has_more {
            rows.pop();
        }

        let mut edges = vec![];
        for node in rows.into_iter() {
            edges.push(Edge {
                cursor: node.to_cursor()?,
                node,
            });
        }

        if self.is_backward() {
            edges = edges.into_iter().rev().collect();
        }

        let page_info = if self.is_backward() {
            let start_cursor = edges.first().map(|e| e.cursor.clone());

            PageInfo {
                has_previous_page: has_more,
                has_next_page: false,
                start_cursor,
                end_cursor: None,
            }
        } else {
            let end_cursor = edges.last().map(|e| e.cursor.clone());
            PageInfo {
                has_previous_page: false,
                has_next_page: has_more,
                start_cursor: None,
                end_cursor,
            }
        };

        Ok(ReadResult { edges, page_info })
    }

    fn build(&mut self) -> (u16, Option<Cursor>) {
        let (limit, cursor) = if self.is_backward() {
            (self.args.last.unwrap_or(40), self.args.before.clone())
        } else {
            (self.args.first.unwrap_or(40), self.args.after.clone())
        };

        if cursor.is_some() {
            let cursor_expr = self.build_cursor_expr(O::bing_keys(), self.qb_args.len() + 1);
            let where_expr = if self.qb.sql().contains(" WHERE ") {
                format!("AND ({cursor_expr})")
            } else {
                format!("WHERE {cursor_expr}")
            };

            self.qb.push(format!(" {where_expr}"));
        }

        let order = match (&self.order, self.is_backward()) {
            (Order::Asc, true) | (Order::Desc, false) => "DESC",
            (Order::Asc, false) | (Order::Desc, true) => "ASC",
        };

        let order_expr = O::bing_keys()
            .iter()
            .map(|k| format!("{k} {order}"))
            .collect::<Vec<_>>()
            .join(", ");

        self.qb
            .push(format!(" ORDER BY {order_expr} LIMIT {}", limit + 1));

        (limit, cursor)
    }

    fn build_cursor_expr(&self, mut keys: Vec<&str>, pos: usize) -> String {
        let sign = match (&self.order, self.is_backward()) {
            (Order::Asc, true) | (Order::Desc, false) => "<",
            (Order::Asc, false) | (Order::Desc, true) => ">",
        };

        let current_key = keys.remove(0);
        let expr = format!("{current_key} {sign} ${pos}");

        if keys.is_empty() {
            return expr;
        }

        format!(
            "{expr} OR ({current_key} = ${pos} AND {})",
            self.build_cursor_expr(keys, pos + 1)
        )
    }

    fn is_backward(&self) -> bool {
        (self.args.last.is_some() || self.args.before.is_some())
            && self.args.first.is_none()
            && self.args.after.is_none()
    }
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
    use crate::{Event, Producer};
    use fake::{
        faker::{
            internet::en::{SafeEmail, Username},
            name::en::Name,
        },
        Dummy, Fake, Faker,
    };
    use rand::{prelude::IndexedRandom, Rng};
    use serde::{Deserialize, Serialize};
    use sqlx::{
        any::{install_default_drivers, Any},
        migrate::MigrateDatabase,
        SqlitePool,
    };
    use std::collections::HashMap;

    type SqliteReader<'args, O> = Query<'args, sqlx::Sqlite, O>;

    fn get_random_event(events: &Vec<Edge<Event>>) -> (u16, Option<Cursor>, usize) {
        let event = events.choose(&mut rand::rng());
        let cursor = event.map(|e| e.cursor.to_owned());
        let pos = event
            .and_then(|e| events.iter().position(|evt| evt.node.id == e.node.id))
            .unwrap_or_default();
        let limit = rand::rng().random_range(0..events.len());

        (limit.try_into().unwrap(), cursor, pos)
    }

    fn test_result(result: ReadResult<Event>, mut edges: Vec<Edge<Event>>, is_backward: bool) {
        let has_more = result.edges.len() < edges.len();
        if has_more {
            edges.pop();
        }

        let page_info = if is_backward {
            edges = edges.into_iter().rev().collect();
            PageInfo {
                has_previous_page: has_more,
                start_cursor: edges.first().map(|e| e.cursor.to_owned()),
                ..Default::default()
            }
        } else {
            PageInfo {
                has_next_page: has_more,
                end_cursor: edges.last().map(|e| e.cursor.to_owned()),
                ..Default::default()
            }
        };
        assert_eq!(result.page_info, page_info);
        assert_eq!(result.edges, edges);
    }

    #[tokio::test]
    async fn forward() {
        let pool = init_data("forward").await.to_owned();
        let events = get_events(&pool, Order::Asc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = all_reader()
                .forward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, false);
        }
    }

    #[tokio::test]
    async fn forward_desc() {
        let pool = init_data("forward_desc").await.to_owned();
        let events = get_events(&pool, Order::Desc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = all_reader()
                .desc()
                .forward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, false);
        }
    }

    #[tokio::test]
    async fn backward() {
        let pool = init_data("backward").await.to_owned();
        let events = get_events(&pool, Order::Desc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = all_reader()
                .backward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, true);
        }
    }

    #[tokio::test]
    async fn backward_desc() {
        let pool = init_data("backward_desc").await.to_owned();
        let events = get_events(&pool, Order::Asc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = all_reader()
                .desc()
                .backward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, true);
        }
    }

    #[tokio::test]
    async fn aggregate_forward() {
        let pool = init_data("aggregate_forward").await.to_owned();
        let events = get_events(&pool, Order::Asc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (id, events) = get_user_events(&events).await;
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = aggregate_reader(id)
                .forward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, false);
        }
    }

    #[tokio::test]
    async fn aggregate_forward_desc() {
        let pool = init_data("aggregate_forward_desc").await.to_owned();
        let events = get_events(&pool, Order::Desc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (id, events) = get_user_events(&events).await;
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = aggregate_reader(id)
                .desc()
                .forward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, false);
        }
    }

    #[tokio::test]
    async fn aggregate_backward() {
        let pool = init_data("aggregate_backward").await.to_owned();
        let events = get_events(&pool, Order::Desc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (id, events) = get_user_events(&events).await;
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = aggregate_reader(id)
                .backward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, true);
        }
    }

    #[tokio::test]
    async fn aggregate_backward_desc() {
        let pool = init_data("aggregate_backward_desc").await.to_owned();
        let events = get_events(&pool, Order::Asc).await;

        for _ in 0..100 {
            let events = events.clone();
            let (id, events) = get_user_events(&events).await;
            let (limit, cursor, pos) = get_random_event(&events);
            let edges = events
                .into_iter()
                .skip(pos + 1)
                .take(limit as usize + 1)
                .collect::<Vec<_>>();

            let result = aggregate_reader(id)
                .desc()
                .backward(limit.try_into().unwrap(), cursor)
                .query(&pool.to_owned())
                .await
                .unwrap();

            test_result(result, edges, true);
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

    fn all_reader<'a>() -> SqliteReader<'a, Event> {
        SqliteReader::new("SELECT * FROM event")
    }

    fn aggregate_reader<'a>(value: impl Into<String>) -> SqliteReader<'a, Event> {
        let value = value.into();
        SqliteReader::new("SELECT * FROM event WHERE aggregate = $1")
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

    async fn get_events(pool: &SqlitePool, order: Order) -> Vec<Edge<Event>> {
        let mut event_version: HashMap<u8, u16> = HashMap::new();
        let order = match order {
            Order::Asc => "ASC",
            Order::Desc => "DESC",
        };

        for _ in 0..100 {
            let user: User = Faker.fake();
            let version = event_version.entry(user.id).or_default();
            let producer = Producer::new(user.id.to_string())
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

        sqlx::query_as::<_, Event>(&format!(
            "select * from event order by timestamp {order}, version {order}, id {order}"
        ))
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|node| Edge {
            cursor: node.to_cursor().unwrap(),
            node,
        })
        .collect::<Vec<_>>()
    }

    async fn get_user_events(events: &Vec<Edge<Event>>) -> (String, Vec<Edge<Event>>) {
        let user: User = Faker.fake();
        let events = events
            .clone()
            .into_iter()
            .filter(|e| e.node.aggregate == user.id.to_string())
            .collect();

        (user.id.to_string(), events)
    }
}
