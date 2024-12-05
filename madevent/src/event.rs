use crate::{FromCursor, ToCursor};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

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

impl FromCursor for Event {
    fn from_cursor<A>(_value: &crate::Cursor) -> A {
        todo!()
    }
}

impl ToCursor for Event {
    fn to_cursor(&self) -> crate::Cursor {
        todo!()
    }
}
