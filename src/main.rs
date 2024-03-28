pub mod events;

use crate::events::InsertEvent;
use notify::{recommended_watcher, RecursiveMode, Watcher};
use std::{path::Path, sync::mpsc::channel};

use mysql_binlog::parse_file;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} /path/to/binlog/file", args[0]);
        std::process::exit(2);
    }

    // Create a channel to receive the events.
    let (tx, rx) = channel();

    // Create a watcher object, delivering raw events.
    // The notification back-end is selected based on the platform.
    let mut watcher = recommended_watcher(tx).unwrap();

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    if let Err(err) = watcher.watch(Path::new(&args[1]), RecursiveMode::Recursive) {
        println!("watcher error {}", err);
    }

    loop {
        match rx.recv() {
            Ok(event) => {
                let data = event.unwrap();
                println!("event: {:?}", data.kind);
                println!("path: {:?}", data.paths[0]);
                for event in parse_file(&data.paths[0])? {
                    if let Ok(event) = event {
                        InsertEvent::process(&event);
                    }
                }
            },
            Err(e) => println!("watch error: {:?}", e),
        }
    }
}
