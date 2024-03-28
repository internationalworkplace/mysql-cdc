use mysql_binlog::event::TypeCode;
use mysql_binlog::BinlogEvent;

pub struct InsertEvent {
    // fields
}

impl InsertEvent {
    pub fn process(event: &BinlogEvent) {
        // Specify the database and table you're interested in
        let target_database: Option<String> = Some("test_one".to_string());
        let target_table: Option<String> = Some("amazing_table".to_string());

        if event.type_code == TypeCode::WriteRowsEventV1
            || event.type_code == TypeCode::WriteRowsEventV2
        {
            if event.schema_name == target_database && event.table_name == target_table {
                println!("{:?}\n", event.rows);
            }
        }
    }
}
