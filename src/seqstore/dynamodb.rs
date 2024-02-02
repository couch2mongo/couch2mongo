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
use crate::settings::config_parser::DynamoDBSettings;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::types::{
    AttributeDefinition,
    AttributeValue,
    BillingMode,
    KeySchemaElement,
    KeyType,
    ScalarAttributeType,
    TableStatus,
};
use aws_sdk_dynamodb::Client;
use std::error::Error;
use tracing::info;

pub struct DynamoDB {
    pub client: Client,
    pub table_name: String,
}

impl DynamoDB {
    /// new creates a new DynamoDB struct.
    ///
    /// # Arguments
    /// * `settings` - A DynamoDBSettings struct
    ///
    /// # Returns
    /// * A DynamoDB struct
    pub async fn new(settings: &DynamoDBSettings) -> DynamoDB {
        let shared_config = aws_config::load_defaults(BehaviorVersion::v2023_11_09()).await;

        let actual_config = match &settings.local_url {
            Some(url) => {
                info!(url = url.as_str(), "using local DynamoDB");

                aws_sdk_dynamodb::config::Builder::from(&shared_config)
                    .endpoint_url(url)
                    .build()
            }
            None => aws_sdk_dynamodb::config::Builder::from(&shared_config).build(),
        };

        let r = DynamoDB {
            client: Client::from_conf(actual_config),
            table_name: settings.table.clone(),
        };

        if settings.create_table {
            r.create_table().await.unwrap();
        }

        r
    }

    /// create_table creates a new DynamoDB table if it doesn't already exist.
    ///
    /// # Arguments
    /// * `self` - A DynamoDB struct
    ///
    /// # Returns
    /// * An empty Result
    pub async fn create_table(&self) -> Result<(), Box<dyn Error>> {
        let r = self
            .client
            .describe_table()
            .table_name(self.table_name.clone())
            .send()
            .await;

        if r.is_err() {
            info!(table_name = self.table_name.as_str(), "creating table");

            self.client
                .create_table()
                .table_name(self.table_name.clone())
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("key")
                        .attribute_type(ScalarAttributeType::S)
                        .build()?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("key")
                        .key_type(KeyType::Hash)
                        .build()?,
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send()
                .await?;
        }

        // Wait for table 'r' to become available, waiting 1 second between checks
        loop {
            info!(
                table_name = self.table_name.as_str(),
                "waiting for table to become available"
            );

            let r = self
                .client
                .describe_table()
                .table_name(self.table_name.clone())
                .send()
                .await?;

            if r.table.unwrap().table_status.unwrap() == TableStatus::Active {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        info!(table_name = self.table_name.as_str(), "table is available");
        Ok(())
    }
}

#[async_trait]
impl SequenceStore for DynamoDB {
    async fn set(&self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        self.client
            .put_item()
            .table_name(self.table_name.clone())
            .item("key", AttributeValue::S(key.to_string()))
            .item("value", AttributeValue::S(value.to_string()))
            .send()
            .await?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let r = self
            .client
            .get_item()
            .table_name(self.table_name.clone())
            .key("key", AttributeValue::S(key.to_string()))
            .consistent_read(true)
            .send()
            .await?;

        match r.item {
            Some(item) => match item.get("value") {
                Some(value) => match value.as_s() {
                    Ok(s) => Ok(Some(s.to_string())),
                    Err(_) => Ok(None),
                },
                None => Ok(None),
            },
            None => Ok(None),
        }
    }
}
