use sqlx::{Arguments, Database, Encode, Executor, FromRow, IntoArguments, QueryBuilder, Type};

pub type SqliteReader<'args> = Reader<'args, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'args>>;

pub struct Reader<'args, DB, A>
where
    DB: Database,
    A: Arguments<'args, Database = DB> + IntoArguments<'args, DB> + Clone,
{
    qb: QueryBuilder<'args, DB>,
    args: A,
}

impl<'args, DB, A> Reader<'args, DB, A>
where
    DB: Database,
    A: Arguments<'args, Database = DB> + IntoArguments<'args, DB> + Clone,
{
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            qb: QueryBuilder::new(sql),
            args: A::default(),
        }
    }

    pub fn bind<Arg>(mut self, arg: Arg) -> Result<Self, sqlx::error::BoxDynError>
    where
        Arg: 'args + Send + Encode<'args, DB> + Type<DB>,
    {
        self.args.add(arg)?;
        Ok(self)
    }

    pub async fn read<O, E>(&'args self, executor: E)
    where
        E: 'args + Executor<'args, Database = DB>,
        O: for<'r> FromRow<'r, DB::Row>,
        O: 'args + Send + Unpin,
    {
        let mut query = sqlx::query_as_with::<_, O, _>(self.qb.sql(), self.args.clone());
        let mut rows = query.fetch_all(executor).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{SqlitePool, any::{install_default_drivers, Any}, migrate::MigrateDatabase};
    use crate::Event;

    #[tokio::test]
    async fn read() {
let pool = get_pool("read").await;
let events = SqliteReader::new("select * from event").read::<Event, _>(&pool).await;
//sqlx::query_as::<sqlx::Sqlite, Event>("").bind()
    }

    async fn get_pool(key: impl Into<String>) -> SqlitePool {
        let key = key.into();
        let dsn = format!("sqlite:../target/reader_{key}.db");

        install_default_drivers();
        let _ = Any::drop_database(&dsn).await;
        Any::create_database(&dsn).await.unwrap();

        let pool = SqlitePool::connect(&dsn).await.unwrap();
        sqlx::migrate!("../migrations").run(&pool).await.unwrap();

        pool
    }
}
