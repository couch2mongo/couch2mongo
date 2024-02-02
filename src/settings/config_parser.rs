// Copyright (c) 2024, Green Man Gaming Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::seqstore::interface::SequenceStore;
use config::{Config, ConfigError, Environment};
use couch_rs::database::Database;
use couch_rs::Client;
use mongodb::options::ClientOptions;
use serde_derive::Deserialize;
use std::error::Error;
use tracing::info;

/// default_as_true returns true for use in serde default attributes.
fn default_as_true() -> bool {
    true
}

fn default_log_level() -> LogLevel {
    LogLevel::Info
}

fn default_log_format() -> LogFormat {
    LogFormat::Compact
}

#[derive(Debug, Deserialize)]
pub enum SequenceStoreInterface {
    Redis,
    DynamoDB,
    Null,
}

#[derive(Debug, Deserialize)]
pub enum LogFormat {
    Compact,
    Json,
}

#[derive(Debug, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl SequenceStoreInterface {
    pub fn as_str(&self) -> &str {
        match *self {
            SequenceStoreInterface::Redis => "redis",
            SequenceStoreInterface::DynamoDB => "dynamodb",
            SequenceStoreInterface::Null => "null",
        }
    }
}

/// RedisSettings is a struct for Redis settings.
#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct RedisSettings {
    pub use_tls: bool,
    pub host: String,
    pub port: u16,
    pub db: u8,
    pub prefix: Option<String>,
    pub password: Option<String>,
}

/// DynamoDBSettings is a struct for DynamoDB settings.
#[derive(Debug, Deserialize, Clone)]
#[allow(unused)]
pub struct DynamoDBSettings {
    pub table: String,
    pub local_url: Option<String>,

    // Create table if it doesn't exist
    #[serde(default = "default_as_true")]
    pub create_table: bool,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    #[serde(default)]
    pub debug: bool,

    // Cloudant/CouchDB source URL
    //
    // eg. http://localhost:5984/
    pub source_url: String,

    // Database to read from
    pub source_database: String,

    // MongoDB Host
    pub mongodb_connect_string: String,

    // MongoDB database
    pub mongodb_database: String,

    // MongoDB collection
    pub mongodb_collection: Option<String>,

    // Use CouchDB field for collection name
    pub mongodb_collection_field: Option<String>,

    // CouchDB username
    pub couchdb_username: Option<String>,

    // CouchDB password
    pub couchdb_password: Option<String>,

    // Optional Key for Sequence Store
    pub sequence_store_key: Option<String>,

    // Sequence Store
    pub sequence_store: SequenceStoreInterface,

    // Redis Settings
    pub redis: Option<RedisSettings>,

    // DynamoDB Settings
    pub dynamodb: Option<DynamoDBSettings>,

    #[serde(default = "default_log_format")]
    pub log_format: LogFormat,

    #[serde(default = "default_log_level")]
    pub log_level: LogLevel,
}

impl Settings {
    pub fn new(config_file: Option<String>) -> Result<Self, ConfigError> {
        let mut config_builder =
            Config::builder().add_source(Environment::with_prefix("couch_stream"));

        match config_file {
            None => {}
            Some(file) => {
                config_builder = config_builder.add_source(config::File::with_name(&file));
            }
        }

        config_builder.build()?.try_deserialize()
    }

    pub fn configure_logging(&self) {
        let x = tracing_subscriber::fmt();

        let y = match self.log_level {
            LogLevel::Debug => x.with_max_level(tracing::Level::DEBUG),
            LogLevel::Info => x.with_max_level(tracing::Level::INFO),
            LogLevel::Warn => x.with_max_level(tracing::Level::WARN),
            LogLevel::Error => x.with_max_level(tracing::Level::ERROR),
        };

        match self.log_format {
            LogFormat::Compact => {
                y.compact().init();
            }
            LogFormat::Json => {
                y.json().init();
            }
        };
    }

    pub async fn get_couchdb_client(&self) -> Result<Client, Box<dyn Error>> {
        let client = Client::new_with_timeout(
            self.source_url.as_str(),
            self.couchdb_username.as_deref(),
            self.couchdb_password.as_deref(),
            Some(10),
        )?;

        Ok(client)
    }

    pub async fn get_couchdb_database(&self) -> Result<Database, Box<dyn Error>> {
        let client = self.get_couchdb_client().await?;
        let db = client.db(self.source_database.as_str()).await?;

        Ok(db)
    }

    pub async fn get_mongodb_client(&self) -> Result<mongodb::Client, Box<dyn Error>> {
        let client_options = ClientOptions::parse(self.mongodb_connect_string.as_str()).await?;
        let client = mongodb::Client::with_options(client_options)?;

        Ok(client)
    }

    pub async fn get_mongodb_database(&self) -> Result<mongodb::Database, Box<dyn Error>> {
        let client = self.get_mongodb_client().await?;
        let db = client.database(self.mongodb_database.as_str());

        Ok(db)
    }

    pub async fn get_sequence_store(&self) -> Result<Box<dyn SequenceStore>, Box<dyn Error>> {
        info!(
            sequence_store = self.sequence_store.as_str(),
            "getting sequence store"
        );

        match self.sequence_store {
            SequenceStoreInterface::Redis => {
                let redis_settings = self.redis.as_ref().unwrap();
                let redis = crate::seqstore::redis::Redis::new(redis_settings);

                Ok(Box::new(redis))
            }
            SequenceStoreInterface::DynamoDB => {
                let dynamodb_settings = self.dynamodb.as_ref().unwrap();
                let dynamodb = crate::seqstore::dynamodb::DynamoDB::new(dynamodb_settings).await;

                Ok(Box::new(dynamodb))
            }
            SequenceStoreInterface::Null => {
                let null = crate::seqstore::null::Null::new();

                Ok(Box::new(null))
            }
        }
    }

    pub fn get_sequence_store_key(&self) -> String {
        self.sequence_store_key
            .clone()
            .unwrap_or(self.mongodb_database.clone())
    }
}
