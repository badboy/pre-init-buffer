use std::sync::RwLock;
use once_cell::sync::Lazy;

use super::{Dispatcher, DispatchError};

const GLOBAL_DISPATCHER_LIMIT: usize = 100;
static GLOBAL_DISPATCHER: Lazy<RwLock<Dispatcher>> = Lazy::new(|| RwLock::new(Dispatcher::new(GLOBAL_DISPATCHER_LIMIT)));

/// Launch a new task on the global dispatch queue.
///
/// The new task will be enqueued immediately.
/// If the queue was already flushed, a background thread will process tasks in the queue (See `flush_init`).
pub fn launch(task: impl Fn() + Send + 'static) -> Result<(), DispatchError> {
    let mut dispatcher = GLOBAL_DISPATCHER.write().unwrap();
    dispatcher.launch(task)
}

/// Start processing queued tasks in the global dispatch queue.
///
/// This function blocks until queued tasks prior to this call are finished.
/// Once the initial queue is empty the dispatcher will wait for new tasks to be launched.
pub fn flush_init() -> Result<(), DispatchError> {
    let mut dispatcher = GLOBAL_DISPATCHER.write().unwrap();
    dispatcher.flush_init()
}
