#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing environment variable `{name}`")]
    EnvVar {
        name: &'static str,
        #[source]
        source: std::env::VarError,
    },

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
