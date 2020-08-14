fn main() {
    env_logger::init();

    log::info!("Enqueuing.");
    for i in 1..=11 {
        log::info!("Enqueue {}", i);
        dispatcher::launch(move || {
            log::info!("Executing task {}", i);
        })
    }

    log::info!("Flushing.");
    dispatcher::flush_init();

    dispatcher::launch(move || {
        log::info!("Another task!");
    });

    log::info!("Done");
}
