use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait SequenceStore {
    async fn set(&self, key: &str, value: &str) -> Result<(), Box<dyn Error>>;

    async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn Error>>;
}
