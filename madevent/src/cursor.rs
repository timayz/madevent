use base64::{
    alphabet,
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{query::QueryAs, Database};

#[derive(thiserror::Error, Debug)]
pub enum Error {
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
