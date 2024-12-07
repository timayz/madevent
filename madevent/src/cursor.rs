use crate::Reader;
use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{Arguments, Database, FromRow, IntoArguments, query::QueryAs};

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

    fn bind_query<'a, DB, O>(
        &self,
        cursor: Self::Cursor,
        query: QueryAs<DB, O, DB::Arguments<'a>>,
    ) -> QueryAs<DB, O, DB::Arguments<'a>>
    where
        DB: Database,
        O: for<'r> FromRow<'r, DB::Row>,
        O: 'a + Send + Unpin,
        O: 'a + BindCursor + ToCursor;

    fn bind_cursor<'a, DB, O>(
        &self,
        value: &Cursor,
        query: QueryAs<DB, O, DB::Arguments<'a>>,
    ) -> Result<QueryAs<DB, O, DB::Arguments<'a>>, sqlx::error::BoxDynError>
    where
        DB: Database,
        O: for<'r> FromRow<'r, DB::Row>,
        O: 'a + Send + Unpin,
        O: 'a + BindCursor + ToCursor,
    {
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine.decode(value).unwrap();
        let cursor = ciborium::from_reader(&decoded[..]).unwrap();

        Ok(self.bind_query(cursor, query))
    }
}
