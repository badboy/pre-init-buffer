fn main() {
    env_logger::init();

    dispatcher::flush_init().unwrap();

    log::info!("Launching first task");
    dispatcher::launch(|| {
        log::info!("First task launching nested one next");

        dispatcher::launch(|| {
            log::info!("Nested task.");

            dispatcher::try_shutdown().unwrap();
        })
        .unwrap();

        log::info!("First task finishes");
    })
    .unwrap();

    log::info!("Joining on worker thread");
    dispatcher::join().unwrap();

    log::info!("Done");
}
