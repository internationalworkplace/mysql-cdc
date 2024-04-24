use rusqlite::{Connection, OptionalExtension, Result};

pub fn create_table_and_insert_record(conn: &Connection) -> Result<()> {
    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS synced_statements (
            id INTEGER PRIMARY KEY,
            last_processed_id INTEGER DEFAULT 0
        )
        ",
        [],
    )?;

    // Insert a record with last_processed_id as 0
    conn.execute(
        "
        INSERT INTO synced_statements (last_processed_id) VALUES (0)
        ",
        [],
    )?;

    Ok(())
}

pub fn get_last_processed_id(conn: &Connection) -> Result<Option<i64>> {
    let mut stmt = conn.prepare("SELECT last_processed_id FROM synced_statements")?;
    if let Some(row) = stmt.query_row([], |row| row.get(0)).optional()? {
        Ok(Some(row))
    } else {
        Ok(Some(0))
    }
}

pub fn update_last_processed_id(conn: &Connection, id: i64) -> Result<()> {
    let mut stmt = conn.prepare(
        "UPDATE synced_statements
        SET last_processed_id = ?",
    )?;
    stmt.execute([&id])?;
    Ok(())
}
