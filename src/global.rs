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

/// Launch a new task on the global dispatch queue.
///
/// The new task will be enqueued immediately.
/// If the queue was already flushed, a background thread will process tasks in the queue (See `flush_init`).
pub fn launch(task: impl FnOnce() + Send + 'static) -> Result<(), DispatchError> {
    guard().launch(task)
}

/// Start processing queued tasks in the global dispatch queue.
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

/// Shutdown the dispatch queue.
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
    log::trace!("join");
    let res = GLOBAL_DISPATCHER
        .write()
        .unwrap()
        .take()
        .map(|dispatcher| dispatcher.join())
        .unwrap();
    log::trace!("END join");
    res
}
