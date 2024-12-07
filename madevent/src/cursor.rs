use crate::Reader;
use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{query::QueryAs, Arguments, Database, FromRow, IntoArguments};

#[derive()]
pub enum Error {}

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

pub trait BindCursor {
    type Cursor: DeserializeOwned;

    fn bing_keys() -> Vec<&'static str>;

    fn bind_query<'q, DB>(
        &self,
        cursor: Self::Cursor,
        args: DB::Arguments<'q>,
    ) -> DB::Arguments<'q>
    where
        DB: Database,
        u32: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        u16: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        String: sqlx::Encode<'q, DB> + sqlx::Type<DB>;

    fn bind_cursor<'q, DB>(
        &self,
        value: &Cursor,
        query: DB::Arguments<'q>,
    ) -> Result<DB::Arguments<'q>, sqlx::error::BoxDynError>
    where
        DB: Database,
        u32: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        u16: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        String: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(value).unwrap();
        let cursor = ciborium::from_reader(&decoded[..]).unwrap();

        Ok(self.bind_query(cursor, query))
    }
}
