fn main() {
    env_logger::init();

    log::info!("Enqueuing.");
    for i in 1..=11 {
        log::info!("Enqueue {}", i);
        dispatcher::launch(move || {
            log::info!("Executing task {}", i);
        }).unwrap();
    }

    log::info!("Flushing.");
    dispatcher::flush_init().unwrap();

    dispatcher::launch(move || {
        log::info!("Another task!");
    }).unwrap();

    log::info!("Done");
}
