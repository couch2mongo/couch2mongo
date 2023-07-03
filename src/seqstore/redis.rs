use crate::seqstore::interface::SequenceStore;
use std::error::Error;

use crate::settings::config_parser::RedisSettings;
use async_trait::async_trait;
use redis::AsyncCommands;

pub struct Redis {
    pub redis: redis::Client,
    pub prefix: Option<String>,
}

impl Redis {
    /// new creates a new Redis struct.
    ///
    /// # Arguments
    /// * `settings` - A RedisSettings struct
    ///
    /// # Returns
    /// * A Redis struct
    pub fn new(settings: &RedisSettings) -> Redis {
        Redis {
            redis: redis::Client::open(Redis::generate_redis_url(settings)).unwrap(),
            prefix: settings.prefix.clone(),
        }
    }

    /// generate_redis_url generates a Redis URL from a RedisSettings struct.
    /// This is used to connect to Redis.
    ///
    /// The URL is in the format:
    /// redis://[username][:password@]host[:port][/db-number]
    ///
    /// See https://docs.rs/redis/0.23.0/redis/#connecting-to-redis for more information.
    ///
    /// # Arguments
    /// * `settings` - A RedisSettings struct
    ///
    /// # Returns
    /// * A Redis URL
    pub fn generate_redis_url(settings: &RedisSettings) -> String {
        format!(
            "{}://{}{}:{}/{}",
            if settings.use_tls { "rediss" } else { "redis" },
            if settings.password.is_some() {
                format!(":{}@", settings.password.as_ref().unwrap())
            } else {
                "".to_string()
            },
            settings.host,
            settings.port,
            settings.db
        )
    }

    fn get_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}:{}", prefix, key),
            None => key.to_string(),
        }
    }
}

/// SequenceStore trait implementation for Redis.
///
/// This allows Redis to be used as a SequenceStore.
#[async_trait]
impl SequenceStore for Redis {
    async fn set(&self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        let mut con = self.redis.get_tokio_connection().await?;
        con.set(self.get_key(key), value).await?;

        return Ok(());
    }

    async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let mut con = self.redis.get_tokio_connection().await?;
        let value: Option<String> = con.get(self.get_key(key)).await?;

        return Ok(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_redis_url_no_password_no_tls() {
        let settings = RedisSettings {
            use_tls: false,
            host: "localhost".to_string(),
            password: None,
            port: 6379,
            db: 0,
            prefix: None,
        };
        assert_eq!(
            Redis::generate_redis_url(&settings),
            "redis://localhost:6379/0"
        );
    }

    #[test]
    fn test_generate_redis_url_with_password_no_tls() {
        let settings = RedisSettings {
            use_tls: false,
            host: "localhost".to_string(),
            password: Some("mypassword".to_string()),
            port: 6379,
            db: 0,
            prefix: None,
        };
        assert_eq!(
            Redis::generate_redis_url(&settings),
            "redis://:mypassword@localhost:6379/0"
        );
    }

    #[test]
    fn test_generate_redis_url_no_password_with_tls() {
        let settings = RedisSettings {
            use_tls: true,
            host: "localhost".to_string(),
            password: None,
            port: 6379,
            db: 0,
            prefix: None,
        };
        assert_eq!(
            Redis::generate_redis_url(&settings),
            "rediss://localhost:6379/0"
        );
    }

    #[test]
    fn test_generate_redis_url_with_password_with_tls() {
        let settings = RedisSettings {
            use_tls: true,
            host: "localhost".to_string(),
            password: Some("mypassword".to_string()),
            port: 6379,
            db: 0,
            prefix: None,
        };
        assert_eq!(
            Redis::generate_redis_url(&settings),
            "rediss://:mypassword@localhost:6379/0"
        );
    }
}
