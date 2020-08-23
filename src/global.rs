use once_cell::sync::{Lazy, OnceCell};
use std::sync::RwLock;

use super::{DispatchError, DispatchGuard, Dispatcher};

const GLOBAL_DISPATCHER_LIMIT: usize = 100;
static GLOBAL_DISPATCHER: Lazy<RwLock<Option<Dispatcher>>> =
    Lazy::new(|| RwLock::new(Some(Dispatcher::new(GLOBAL_DISPATCHER_LIMIT))));

fn guard() -> &'static DispatchGuard {
    static GLOBAL_GUARD: OnceCell<DispatchGuard> = OnceCell::new();

    GLOBAL_GUARD.get_or_init(|| {
        let lock = GLOBAL_DISPATCHER.read().unwrap();
        lock.as_ref().unwrap().guard()
    })
}

/// Launches a new task on the global dispatch queue.
///
/// The new task will be enqueued immediately.
/// If the pre-init queue was already flushed,
/// the background thread will process tasks in the queue (see [`flush_init`]).
///
/// This will not block.
///
/// [`flush_init`]: fn.flush_init.html
pub fn launch(task: impl FnOnce() + Send + 'static) -> Result<(), DispatchError> {
    guard().launch(task)
}

/// Starts processing queued tasks in the global dispatch queue.
///
/// This function blocks until queued tasks prior to this call are finished.
/// Once the initial queue is empty the dispatcher will wait for new tasks to be launched.
pub fn flush_init() -> Result<(), DispatchError> {
    GLOBAL_DISPATCHER
        .write()
        .unwrap()
        .as_mut()
        .map(|dispatcher| dispatcher.flush_init())
        .unwrap()
}

/// Shuts down the dispatch queue.
///
/// This will initiate a shutdown of the worker thread
/// and no new tasks will be processed after this.
/// It will not block on the worker thread.
pub fn try_shutdown() -> Result<(), DispatchError> {
    guard().shutdown()
}

/// Waits for the worker thread to finish and finishes the dispatch queue.
///
/// You need to call `try_shutdown` to initiate a shutdown of the queue.
pub fn join() -> Result<(), DispatchError> {
    GLOBAL_DISPATCHER
        .write()
        .unwrap()
        .take()
        .map(|dispatcher| dispatcher.join())
        .unwrap()
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use super::*;

    // We can only test this once, as it is a global resource which we can't reset.
    #[test]
    fn global_fills_up_in_order_and_works() {
        let _ = env_logger::builder().is_test(true).try_init();

        let result = Arc::new(Mutex::new(vec![]));

        for i in 1..=100 {
            let result = Arc::clone(&result);
                launch(move || {
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        {
            let result = Arc::clone(&result);
            let err = launch(move || {
                result.lock().unwrap().push(150);
            });
            assert_eq!(Err(DispatchError::QueueFull), err);
        }

        flush_init().unwrap();

        {
            let result = Arc::clone(&result);
                launch(move || {
                    result.lock().unwrap().push(200);
                })
                .unwrap();
        }

        try_shutdown().unwrap();
        join().unwrap();

        let mut expected = (1..=100).collect::<Vec<_>>();
        expected.push(200);
        assert_eq!(&*result.lock().unwrap(), &expected);
    }
}
