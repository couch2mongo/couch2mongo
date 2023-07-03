use crate::seqstore::interface::SequenceStore;
use async_trait::async_trait;
use std::error::Error;
use std::sync::{Arc, RwLock};

/// Null is a dummy struct that implements the SeqStore trait.
/// It is used when the user wants to not store any sequence data.
///
/// It uses a Thread-Safe Reference-Counted Write-Locked Option<String> to store the value.
/// This allows the main code to just 'work' as if it was running against a real store.
///
/// We don't need the key - as we don't actually use more than one stream per
/// running application.
pub struct Null {
    pub v: Arc<RwLock<Option<String>>>,
}

impl Null {
    pub fn new() -> Self {
        Null {
            v: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl SequenceStore for Null {
    async fn set(&self, _key: &str, _value: &str) -> Result<(), Box<dyn Error>> {
        self.v
            .write()
            .expect("unable to write to v")
            .replace(_value.to_string());
        Ok(())
    }

    async fn get(&self, _key: &str) -> Result<Option<String>, Box<dyn Error>> {
        return Ok(self
            .v
            .read()
            .expect("unable to read from v")
            .clone()
            .map_or_else(|| None, Some));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_null_sequence_store() {
        let rt = Runtime::new().unwrap();
        let null_store = Null::new();

        rt.block_on(async {
            // Test set
            let set_res = null_store.set("test_key", "test_value").await;
            assert!(set_res.is_ok());

            // Test get
            let get_res = null_store.get("test_key").await;
            match get_res {
                Ok(value) => assert_eq!(value, Some("test_value".to_string())),
                Err(_) => panic!("get operation failed"),
            }
        });
    }
}
