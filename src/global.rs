use std::sync::RwLock;
use once_cell::sync::{OnceCell, Lazy};

use super::{Dispatcher, DispatchGuard, DispatchError};

const GLOBAL_DISPATCHER_LIMIT: usize = 100;
static GLOBAL_DISPATCHER: Lazy<RwLock<Option<Dispatcher>>> = Lazy::new(|| RwLock::new(Some(Dispatcher::new(GLOBAL_DISPATCHER_LIMIT))));

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
pub fn launch(task: impl Fn() + Send + 'static) -> Result<(), DispatchError> {
    guard().launch(task)
}

/// Start processing queued tasks in the global dispatch queue.
///
/// This function blocks until queued tasks prior to this call are finished.
/// Once the initial queue is empty the dispatcher will wait for new tasks to be launched.
pub fn flush_init() -> Result<(), DispatchError> {
    GLOBAL_DISPATCHER.write().unwrap().as_mut().map(|dispatcher| dispatcher.flush_init()).unwrap()
}

/// Shutdown the dispatch queue.
///
/// This will initiate a shutdown of the worker thread
/// and block until all enqueued tasks are finished.
///
/// The global queue won't be usable after this.
/// Subsequent calls to `launch` will panic.
pub fn try_shutdown() -> Result<(), DispatchError> {
    guard().shutdown()
}

pub fn join() -> Result<(), DispatchError> {
    log::trace!("join");
    let res = GLOBAL_DISPATCHER.write().unwrap().take().map(|dispatcher| dispatcher.join()).unwrap();
    log::trace!("END join");
    res
}
