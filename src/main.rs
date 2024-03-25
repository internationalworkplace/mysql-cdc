extern crate notify;

use notify::{raw_watcher, RawEvent, RecursiveMode, Watcher};
use std::{sync::mpsc::channel, time::Duration};

fn main() {
    // Create a channel to receive the events.
    let (tx, rx) = channel();

    // Create a watcher object, delivering raw events.
    // The notification back-end is selected based on the platform.
    let mut watcher = raw_watcher(tx).unwrap();

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher
        .watch(
            "/Users/nate/Library/Application Support/com.tinyapp.DBngin/Engines/mysql/F3E67C2D-C5E7-4419-8C7A-FF988C512453/binlog.000001",
            RecursiveMode::Recursive,
        )
        .unwrap();

    std::thread::spawn(move || match rx.recv() {
        Ok(RawEvent {
            path: Some(path),
            op: Ok(op),
            cookie,
        }) => {
            println!("{:?} {:?} ({:?})", op, path, cookie)
        }
        Ok(event) => println!("broken event: {:?}", event),
        Err(e) => println!("watch error: {:?}", e),
    });

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
