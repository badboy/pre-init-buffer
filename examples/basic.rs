use dispatcher::Dispatcher;
use std::{thread, time::Duration};

fn main() {
    env_logger::init();

    let mut dispatcher = Dispatcher::new(100);

    log::info!("Enqueuing.");
    for i in 1..=10 {
        log::info!("Enqueue {}", i);
        dispatcher
            .launch(move || {
                log::info!("Executing task {}", i);
                thread::sleep(Duration::from_millis(50));
            })
            .unwrap();
    }

    log::info!("Flushing.");
    dispatcher.flush_init().unwrap();

    dispatcher
        .launch(move || {
            log::info!("Another task!");
        })
        .unwrap();

    dispatcher.try_shutdown().unwrap();
    dispatcher.join().unwrap();
    log::info!("Done");
}
