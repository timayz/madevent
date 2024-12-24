use crate::{BindCursor, ToCursor};
use serde::{Deserialize, Serialize};
use sqlx::{query::QueryAs, Database, Encode, FromRow, Type};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub struct Event {
    pub id: String,
    pub name: String,
    pub aggregate: String,
    pub version: u16,
    pub data: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub topic: String,
    pub tenant: Option<String>,
    pub timestamp: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventCursor {
    pub i: String,
    pub v: u16,
    pub t: u32,
}

impl Event {
    pub fn to_data<D: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<Option<D>, ciborium::de::Error<std::io::Error>> {
        if self.name != std::any::type_name::<D>() {
            return Ok(None);
        }

        ciborium::from_reader(&self.data[..])
    }

    pub fn to_metadata<M: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<Option<M>, ciborium::de::Error<std::io::Error>> {
        match &self.metadata {
            Some(metadata) => ciborium::from_reader(&metadata[..]),
            _ => Ok(None),
        }
    }
}

impl<'q, DB: Database> BindCursor<'q, DB> for Event
where
    u16: Encode<'q, DB> + Type<DB>,
    u32: Encode<'q, DB> + Type<DB>,
    String: Encode<'q, DB> + Type<DB>,
{
    type Cursor = EventCursor;

    fn bing_keys() -> Vec<&'static str> {
        vec!["timestamp", "version", "id"]
    }

    fn bind_query<O>(
        cursor: Self::Cursor,
        query: QueryAs<'q, DB, O, <DB as Database>::Arguments<'q>>,
    ) -> QueryAs<'q, DB, O, <DB as Database>::Arguments<'q>> {
        query.bind(cursor.t).bind(cursor.v).bind(cursor.i)
    }
}

impl ToCursor for Event {
    type Cursor = EventCursor;

    fn serialize_cursor(&self) -> EventCursor {
        EventCursor {
            i: self.id.clone(),
            v: self.version,
            t: self.timestamp,
        }
    }
}
