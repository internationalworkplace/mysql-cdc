pub mod events;

use crate::events::InsertEvent;

use mysql_binlog::parse_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} /path/to/binlog/file", args[0]);
        std::process::exit(2);
    }

    for event in parse_file(&args[1])? {
        if let Ok(event) = event {
            InsertEvent::process(&event);
        }
    }
    Ok(())
}
