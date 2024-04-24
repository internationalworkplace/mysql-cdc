use mysql_binlog::BinlogEvent;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use std::error::Error;

//
pub struct InsertEvent;

impl InsertEvent {
    pub fn process(event: &BinlogEvent) -> Result<(), Box<dyn Error>> {
        for _row in &event.rows {
            let producer = Self::get_producer();
            let _ = tokio::runtime::Runtime::new()?.block_on(Self::produce(&producer, &event));
        }

        Ok(())
    }

    async fn produce(producer: &FutureProducer, event: &BinlogEvent) -> Result<(), Box<dyn Error>> {
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
}
