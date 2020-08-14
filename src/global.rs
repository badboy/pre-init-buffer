use std::sync::RwLock;
use once_cell::sync::Lazy;

use super::Dispatcher;

const GLOBAL_DISPATCHER_LIMIT: usize = 10;
static GLOBAL_DISPATCHER: Lazy<RwLock<Dispatcher>> = Lazy::new(|| RwLock::new(Dispatcher::new(GLOBAL_DISPATCHER_LIMIT)));

pub fn launch(task: impl Fn() + Send + 'static) {
    let mut dispatcher = GLOBAL_DISPATCHER.write().unwrap();
    dispatcher.launch(task)
}

pub fn flush_init() {
    let mut dispatcher = GLOBAL_DISPATCHER.write().unwrap();
    dispatcher.flush_init()
}

pub fn finish() {
    let mut dispatcher = GLOBAL_DISPATCHER.write().unwrap();
    dispatcher.finish()
}
