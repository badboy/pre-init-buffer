use std::{
    sync::{Arc, Barrier, Mutex},
    thread,
    time::Duration,
};

use dispatcher::Dispatcher;

#[test]
fn concurrent_queueing() {
    let _ = env_logger::builder().is_test(true).try_init();

    let mut dispatcher = Dispatcher::new(5);

    let result = Arc::new(Mutex::new(vec![]));

    {
        let result = Arc::clone(&result);
        dispatcher
            .launch(move || {
                result.lock().unwrap().push("000-main-pre-init");
            })
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let t = {
        let result = Arc::clone(&result);
        let barrier = Arc::clone(&barrier);
        let dispatcher = dispatcher.guard();

        thread::spawn(move || {
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push("001-thread-pre-init");
                })
                .unwrap();
            // Ensure we finish before the flush
            barrier.wait();
        })
    };

    barrier.wait();

    dispatcher.flush_init().unwrap();

    {
        let result = Arc::clone(&result);
        dispatcher
            .launch(move || {
                result.lock().unwrap().push("002-main-post");
            })
            .unwrap();
    }

    let t2 = {
        let result = Arc::clone(&result);
        let dispatcher = dispatcher.guard();
        thread::spawn(move || {
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push("003-thread-post");
                })
                .unwrap();
        })
    };

    thread::sleep(Duration::from_millis(10));

    dispatcher.try_shutdown().unwrap();
    dispatcher.join().unwrap();
    t.join().unwrap();
    t2.join().unwrap();

    let data = result.lock().unwrap().clone();
    let expected: &[&str] = &[
        "000-main-pre-init",
        "001-thread-pre-init",
        "002-main-post",
        "003-thread-post",
    ];
    assert_eq!(data, expected);
}
