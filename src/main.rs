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

mod seqstore;
mod settings;

use crate::settings::config_parser::Settings;
use bson::Document;
use clap::{command, Parser};
use couch_rs::types::changes::ChangeEvent;
use futures_util::StreamExt;
use mongodb::options::ReplaceOptions;
use std::error::Error;
use std::fmt::Debug;
use tracing::{debug, info, instrument};

/// ChangeEventDetails is a trait that provides some helper methods for
/// ChangeEvent.
trait ChangeEventDetails {
    /// is_design_document returns true if the ChangeEvent is a design document.
    fn is_design_document(&self) -> bool;
}

/// ChangeEventDetails is implemented for ChangeEvent.
impl ChangeEventDetails for ChangeEvent {
    /// is_design_document returns true if the ChangeEvent is a design document.
    fn is_design_document(&self) -> bool {
        self.id.starts_with("_design")
    }
}

#[derive(Parser, Debug)]
#[command(author = None, version = None, about = "CouchDB to MongoDB Streamer", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[instrument]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let config_file = args.config;

    let s = Settings::new(Some(config_file.to_string()));
    match s {
        Ok(_) => {}
        Err(e) => {
            panic!("unable to load config: {}", e);
        }
    }

    let unwrapped_settings = s.unwrap();
    unwrapped_settings.configure_logging();

    let sequence_store = unwrapped_settings.get_sequence_store().await?;
    let mut current_sequence = sequence_store
        .get(&unwrapped_settings.get_sequence_store_key())
        .await?;

    let db = unwrapped_settings.get_couchdb_database().await?;

    let mut changes = db.changes(current_sequence.clone().map(serde_json::Value::String));
    changes.set_infinite(true);

    let db = unwrapped_settings.get_mongodb_database().await?;

    let upsert_options = ReplaceOptions::builder().upsert(true).build();

    while let Some(change) = changes.next().await {
        let change_event = change.unwrap();

        // Always test to see if the underlying store changed beneath us
        let test_current_sequence = sequence_store
            .get(&unwrapped_settings.get_sequence_store_key())
            .await?;

        // compare test_current_sequence to current_sequence
        if test_current_sequence != current_sequence {
            panic!(
                "sequence mismatch: {:?} != {:?}",
                test_current_sequence, current_sequence
            );
        }

        debug!(
            id = change_event.id.as_str(),
            seq = change_event.seq.as_str()
        );

        if change_event.is_design_document() {
            info!(
                id = change_event.id.as_str(),
                seq = change_event.seq.as_str(),
                "design document"
            );
            continue;
        }

        let couch_document = change_event.doc.unwrap();
        let bson_value = bson::to_bson(&couch_document).unwrap();
        let bson_document = bson_value.as_document().unwrap();

        let document_id = bson::doc! { "_id": bson_document.get("_id").unwrap() };

        let collection =
            db.collection::<Document>(collection_name(&unwrapped_settings, bson_document).as_str());

        if bson_document.get("_deleted").is_some() {
            info!(
                id = change_event.id.as_str(),
                seq = change_event.seq.as_str(),
                collection = collection.name(),
                "deleting document",
            );
            collection.delete_one(document_id, None).await?;
            continue;
        }

        info!(
            id = change_event.id.as_str(),
            seq = change_event.seq.as_str(),
            collection = collection.name(),
            "replacing document",
        );

        let result = collection
            .replace_one(
                document_id,
                bson_document.clone(),
                Some(upsert_options.clone()),
            )
            .await?;

        if result.upserted_id.is_some() {
            info!(
                id = change_event.id.as_str(),
                seq = change_event.seq.as_str(),
                collection = collection.name(),
                "document inserted",
            );
        };

        sequence_store
            .set(
                &unwrapped_settings.get_sequence_store_key(),
                change_event.seq.as_str().unwrap(),
            )
            .await?;

        current_sequence = Some(change_event.seq.as_str().unwrap().to_string());
    }

    Ok(())
}

/// Returns the collection name to use for the document.
///
/// If the `mongodb_collection_field` setting is set, then the value of that field is used as the
/// collection name. If the `mongodb_collection` setting is set, then the value of that setting is
/// used as the collection name. If neither setting is set, then the value of the `source_database`
/// setting is used as the collection name.
///
/// # Arguments
///
/// * `unwrapped_settings` - The settings object.
/// * `bson_document` - The BSON document.
///
/// # Returns
///
/// * `String` - The collection name to use.
fn collection_name(unwrapped_settings: &Settings, bson_document: &Document) -> String {
    let c = match unwrapped_settings.mongodb_collection {
        Some(ref collection) => collection.as_str(),
        None => unwrapped_settings.source_database.as_str(),
    };

    match unwrapped_settings.mongodb_collection_field {
        Some(ref field) => match bson_document.get(field) {
            Some(_) => bson_document.get(field).unwrap().as_str().unwrap(),
            None => c,
        },
        None => match unwrapped_settings.mongodb_collection {
            Some(ref collection) => collection.as_str(),
            None => c,
        },
    }
    .to_string()
}
