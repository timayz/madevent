use crate::{BindCursor, ToCursor};
use serde::{Deserialize, Serialize};
use sqlx::{query::QueryAs, Arguments, Database, FromRow, IntoArguments};

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

impl BindCursor for Event {
    type Cursor = EventCursor;

    fn bing_keys() -> Vec<&'static str> {
        vec!["timestamp", "version", "id"]
    }

    fn bind_query<'q, DB>(
        &self,
        cursor: Self::Cursor,
        mut args: DB::Arguments<'q>,
    ) -> DB::Arguments<'q>
    where
        DB: Database,
        u32: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        u16: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        String: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        args.add(cursor.t).unwrap();
        args.add(cursor.v).unwrap();
        args.add(cursor.i).unwrap();
        args
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
