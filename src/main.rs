extern crate notify;

use notify::{recommended_watcher, RecursiveMode, Watcher};
use std::{path::Path, sync::mpsc::channel};

fn main() {
    // Create a channel to receive the events.
    let (tx, rx) = channel();

    // Create a watcher object, delivering raw events.
    // The notification back-end is selected based on the platform.
    let mut watcher = recommended_watcher(tx).unwrap();

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    if let Err(err) = watcher.watch(Path::new("/Users/nate/logs"), RecursiveMode::Recursive) {
        println!("watcher error {}", err);
    }

    loop {
        match rx.recv() {
            Ok(event) => println!("broken event: {:?}", event),
            Err(e) => println!("watch error: {:?}", e),
        }
    }
}
