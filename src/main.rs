mod events;
mod statements;

use crate::events::InsertEvent;
use dotenv::dotenv;
use mysql_binlog::event::TypeCode;
use mysql_binlog::{parse_file, BinlogEvent};
use notify::{recommended_watcher, RecursiveMode, Watcher};
use rusqlite::Connection;
use statements::{create_table_and_insert_record, get_last_processed_id, update_last_processed_id};
use std::{path::Path, sync::mpsc::channel};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let bin_path = std::env::var("BIN_LOG_PATH").expect("BIN_LOG_PATH must be set.");
    let database_list = std::env::var("TARGET_DATABASES").expect("TARGET_DATABASES must be set.");
    let table_list = std::env::var("TARGET_TABLES").expect("TARGET_TABLES must be set.");

    let sync_cutoff_unix_timestamp = std::env::var("SYNC_CUTOFF_TIMESTAMP_UNIX").unwrap_or("0".to_string()).parse::<i32>().unwrap_or(0);

    // Create SQLite connection
    let conn = Connection::open("sqlite.db")?;
    // Create table if not exists
    create_table_and_insert_record(&conn)?;

    // Create a channel to receive the events.
    let (tx, rx) = channel();

    // Create a watcher object, delivering raw events.
    // The notification back-end is selected based on the platform.
    let mut watcher = recommended_watcher(tx).unwrap();

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    if let Err(err) = watcher.watch(Path::new(bin_path.as_str()), RecursiveMode::Recursive) {
        println!("watcher error {}", err);
    }
    loop {
        match rx.recv() {
            Ok(event) => {
                let data = event.unwrap();
                let last_processed_id = get_last_processed_id(&conn)?;

                let target_databases: Vec<&str> = database_list.split(',').map(|s| s.trim()).collect();
                let target_tables: Vec<&str> = table_list.split(',').map(|s| s.trim()).collect();

                println!("Last processed ID {:?}", last_processed_id);
                println!("sync_cutoff_unix_timestamp {:?}", sync_cutoff_unix_timestamp);

                for (index, event_result) in parse_file(&data.paths[0])?.enumerate() {
                    match event_result {
                        Ok(event) => {
                            if 
                                matches_target_type_and_table(&event, &target_databases, &target_tables)
                                && is_event_after_cutoff(&event, sync_cutoff_unix_timestamp)
                            {
                                println!("Event {}: {:?} : {:?}", index, event, last_processed_id);

                                // TODO: Need to make the SQLite DB store the last timestamp for every specified table individually.
                                // Extract the ID column from the event
                                if let Some(id) = extract_id(&event) {
                                    // Check if the event ID is greater than the last processed ID
                                    if let Some(last_id) = last_processed_id {
                                        if id <= last_id {
                                            println!(
                                                "Skipping already processed event with ID {}",
                                                id
                                            );
                                            continue;
                                        }
                                    }

                                    //Process the event with the extracted ID
                                    let _ = InsertEvent::process(&event);

                                    //Update the last processed ID
                                    if let Err(err) = update_last_processed_id(&conn, id) {
                                        println!("Error updating last processed ID: {:?}", err);
                                    }
                                }
                            }
                        }
                        Err(e) => println!("Error parsing event {}: {:?}", index, e),
                    }
                }
            }
            Err(e) => println!("watch error: {:?}", e),
        }
    }
}

// Function to extract the ID column from the event
fn extract_id(event: &BinlogEvent) -> Option<i64> {
    for row in &event.rows {
        if let Some(cols) = row.cols() {
            if let Some(id_value) = cols.get(0) {
                if let Some(id) = match id_value {
                    Some(mysql_binlog::value::MySQLValue::SignedInteger(value)) => Some(*value),
                    _ => None,
                } {
                    return Some(id);
                }
            }
        }
    }
    None
}

fn matches_target_type_and_table(event: &BinlogEvent, target_databases: &[&str], target_tables: &[&str]) -> bool {
    if let Some(_schema) = event.schema_name.as_deref() {
        if let Some(_table) = event.table_name.as_deref() {
            let schema = event.schema_name.as_deref().unwrap();
            let table = event.table_name.as_deref().unwrap();

            if schema == "staging_analytics" {
                return false;
            }

            return (event.type_code == TypeCode::WriteRowsEventV1 || event.type_code == TypeCode::WriteRowsEventV2)
                && (target_databases.contains(&schema) && target_tables.contains(&table))
        }
    }
    false
}

fn is_event_after_cutoff(event: &BinlogEvent, sync_cutoff_unix_timestamp: i32) -> bool {
    return event.timestamp > sync_cutoff_unix_timestamp.try_into().unwrap()
}