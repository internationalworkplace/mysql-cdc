use mysql_binlog::{event::RowEvent, value::MySQLValue, BinlogEvent};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde_json::{json, Value};
use std::error::Error;

//
pub struct InsertEvent;

impl InsertEvent {
    pub fn process(event: &BinlogEvent) -> Result<(), Box<dyn Error>> {
        let producer = Self::get_producer();
        let database = event.schema_name.as_ref().map(|s| s.as_str()).unwrap_or_default();
        let table = event.table_name.as_ref().map(|s| s.as_str()).unwrap_or_default();

        // Concatenate database name and table name
        let schema_and_table = format!("{}_{}", database, table);
        for _row in &event.rows {
            // Call the appropriate mapping function based on the schema
            let json_data = match schema_and_table.as_str() {
                "staging_lrs_lrs_statement" => Self::map_staging_lrs_lrs_statements(_row),
                "staging_lms_user_details" => Self::map_staging_lms_user_details(_row),
                // Add more schemas as needed
                _ => {
                    // Handle case when no mapping function is found
                    println!("No mapping function found for schema and table: {}", schema_and_table);
                    continue;
                }
            };

            // Convert JSON data to string
            let json_string = serde_json::to_string(&json_data)?;

            let _ = tokio::runtime::Runtime::new()?.block_on(Self::produce(&producer, json_string));
        }

        Ok(())
    }

    async fn produce(producer: &FutureProducer, event: String) -> Result<(), Box<dyn Error>> {
        let kafka_topic = std::env::var("KAFKA_TOPIC").expect("KAFKA_TOPIC must be set.");
        let topic_string = kafka_topic.to_string();

        // Convert the event to a payload format that Kafka expects
        let payload = format!("{:?}", event); // Example: Convert event to JSON format
        let data = FutureRecord::to(&topic_string)
            .payload(&payload)
            .key(&topic_string);
        match producer.send(data, std::time::Duration::from_secs(0)).await {
            Ok(_) => {
                println!("Message successfully sent to Kafka");
                Ok(())
            }
            Err(e) => {
                println!("Error sending message to Kafka: {:?}", e);
                Err("Error sending message to Kafka".into()) // Return a generic error message
            }
        }
    }

    fn get_producer() -> FutureProducer {
        let bootstrap_servers =
            std::env::var("BOOTSTRAP_SERVERS").expect("BOOTSTRAP_SERVERS must be set.");
        let sasl_mechanism = std::env::var("SASL_MECHANISM").expect("SASL_MECHANISM must be set.");
        let security_protocol =
            std::env::var("SECURITY_PROTOCOL").expect("SECURITY_PROTOCOL must be set.");
        let username = std::env::var("SASL_USERNAME").expect("SASL_USERNAME must be set.");
        let password = std::env::var("SASL_PASSWORD").expect("SASL_PASSWORD must be set.");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("sasl.mechanism", sasl_mechanism)
            .set("security.protocol", security_protocol)
            // .set("message.timeout.ms", "5000")
            .set("sasl.username", username)
            .set("sasl.password", password)
            .create()
            .expect("Failed to create producer");
        producer
    }
    
    fn map_staging_lrs_lrs_statements(row_event: &RowEvent) -> Value {
        // Has to be in the exact order of the row values.
        let column_names = vec![
            "id".to_string(),
            "uuid".to_string(),
            "parent_id".to_string(),
            "actor_id".to_string(),
            "verb_id".to_string(),
            "target_id".to_string(),
            "result_id".to_string(),
            "context_id".to_string(),
            "timestamp".to_string(),
            "stored_on".to_string(),
            "authority_id".to_string(),
            "version".to_string()
        ];
        
        return Self::process_row_event(row_event, column_names);
    }

    fn map_staging_lms_user_details(row_event: &RowEvent) -> Value {
        // Has to be in the exact order of the row values.

        let column_names = vec![
            "user_id".to_string(),
        ];
        
        return Self::process_row_event(row_event, column_names);
    }

    fn process_row_event(row_event: &RowEvent, column_names: Vec<String>) -> Value {
        if let RowEvent::NewRow { cols } = row_event {
            let mut json_data = json!({});
    
            for (column_index, column_value) in cols.iter().enumerate() {
                if let Some(column_name) = column_names.get(column_index) {
                    // Convert column value to JSON value
                    let value = match column_value {
                        Some(MySQLValue::SignedInteger(val)) => json!(val),
                        Some(MySQLValue::Float(val)) => json!(val),
                        Some(MySQLValue::Double(val)) => json!(val),
                        Some(MySQLValue::String(val)) => json!(val),
                        Some(MySQLValue::Enum(val)) => json!(val),
                        // Handle other variants as needed
                        _ => json!(null), // Handle null values or unsupported types
                    };
    
                    // Add column and value to JSON object
                    json_data[column_name] = value;
                }
            }
    
            return json_data;
        }
    
        // Handle other variants or return a default JSON value if needed
        json!({})
    }
}
