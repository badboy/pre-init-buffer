//! A global dispatcher queue.
//!
//! # Example - Local Dispatch queue
//!
//! ```rust
//! # use dispatcher::Dispatcher;
//! let mut dispatcher = Dispatcher::new(10);
//!
//! dispatcher.launch(|| {
//!     println!("An early task to be queued up");
//! });
//!
//! // Ensure the dispatcher queue is being worked on.
//! dispatcher.flush_init();
//!
//! dispatcher.launch(|| {
//!     println!("Executing expensive task");
//!     // Run your expensive task in a separate thread.
//! });
//!
//! dispatcher.launch(|| {
//!     println!("A second task that's executed sequentially, but off the main thread.");
//! });
//!
//! // Finally stop the dispatcher
//! dispatcher.try_shutdown().unwrap();
//! # dispatcher.join().unwrap();
//! ```
//!
//! # Example - Global Dispatch queue
//!
//! The global dispatch queue is pre-configured with a maximum queue size of 100 tasks.
//!
//! ```rust
//! // Ensure the dispatcher queue is being worked on.
//! dispatcher::flush_init();
//!
//! dispatcher::launch(|| {
//!     println!("Executing expensive task");
//!     // Run your expensive task in a separate thread.
//! });
//!
//! dispatcher::launch(|| {
//!     println!("A second task that's executed sequentially, but off the main thread.");
//! });
//! ```

use std::mem;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, unbounded, SendError, Sender, TrySendError};
use thiserror::Error;

pub use global::*;

mod global;

/// The command a worker should execute.
enum Command {
    /// A task is a user-defined function to run.
    Task(Box<dyn FnOnce() + Send>),

    /// Swap the channel
    Swap(Sender<()>),

    /// Signal the worker to finish work and shut down.
    Shutdown,
}

#[derive(Error, Debug, PartialEq)]
pub enum DispatchError {
    #[error("The worker panicked while running a task")]
    WorkerPanic,

    #[error("Maximum queue size reached")]
    QueueFull,

    #[error("Pre-init buffer was already flushed")]
    AlreadyFlushed,

    #[error("Failed to send command to worker thread")]
    SendError,

    #[error("Failed to receive from channel")]
    RecvError(#[from] crossbeam_channel::RecvError),
}

impl From<TrySendError<Command>> for DispatchError {
    fn from(err: TrySendError<Command>) -> Self {
        match err {
            TrySendError::Full(_) => DispatchError::QueueFull,
            _ => DispatchError::SendError,
        }
    }
}

impl<T> From<SendError<T>> for DispatchError {
    fn from(err: SendError<T>) -> Self {
        match err {
            _ => DispatchError::SendError,
        }
    }
}

#[derive(Clone)]
pub struct DispatchGuard {
    preinit: Sender<Command>,
    sender: Sender<Command>,
}

impl DispatchGuard {
    pub fn launch(&self, task: impl FnOnce() + Send + 'static) -> Result<(), DispatchError> {
        log::trace!("Launching task on the guard");

        let task = Command::Task(Box::new(task));
        self.send(task)
    }

    pub fn shutdown(&self) -> Result<(), DispatchError> {
        self.send(Command::Shutdown)
    }

    fn send(&self, task: Command) -> Result<(), DispatchError> {
        log::trace!("Sending on preinit channel");
        let task = match self.preinit.try_send(task) {
            Ok(()) => {
                log::trace!("preinit send succeeded.");
                return Ok(());
            }
            Err(TrySendError::Full(_)) => return Err(DispatchError::QueueFull),
            Err(TrySendError::Disconnected(t)) => t,
        };

        log::trace!("Sending on unbounded channel");
        self.sender.send(task)?;
        Ok(())
    }
}

/// A dispatcher.
///
/// Run expensive processing tasks sequentially off the main thread.
/// Tasks are processed in a single separate thread in the order they are submitted.
/// The dispatch queue will enqueue tasks while not flushed, up to the maximum queue size.
/// Processing will start after flushing once, processing already enqueued tasks first, then
/// waiting for further tasks to be enqueued.
///
/// # Example
///
/// ```rust
/// # use dispatcher::Dispatcher;
/// let mut dispatcher = Dispatcher::new(5);
/// dispatcher.flush_init();
///
/// dispatcher.launch(|| {
///     println!("A task of the main thread");
/// }).unwrap();
///
/// dispatcher.try_shutdown().unwrap();
/// dispatcher.join().unwrap();
/// ```
pub struct Dispatcher {
    block_sender: Option<Sender<()>>,
    preinit_sender: Sender<Command>,
    sender: Sender<Command>,
    worker: Option<JoinHandle<()>>,
}

impl Dispatcher {
    /// Create a new dispatcher with a maximum queue size.
    ///
    /// Launched tasks won't run until `flush_init` is called.
    pub fn new(max_queue_size: usize) -> Self {
        let (block_sender, block_rx) = bounded(1);
        let (preinit_sender, preinit_rx) = bounded(max_queue_size);
        let (sender, mut unbounded_rx) = unbounded();

        let worker = thread::spawn(move || {
            log::trace!("Worker started in {:?}", thread::current().id());

            match block_rx.recv() {
                Ok(()) => log::trace!("{:?}: Unblocked. Processing queue.", thread::current().id()),
                Err(e) => {
                    log::trace!(
                        "{:?}: Failed to receive preinit task in worker: Error: {:?}",
                        thread::current().id(),
                        e
                    );
                    return;
                }
            }

            let mut rx = preinit_rx;
            loop {
                use Command::*;

                match rx.recv() {
                    Ok(Shutdown) => {
                        log::trace!("{:?}: Received `Shutdown`", thread::current().id());
                        break;
                    }
                    Ok(Task(f)) => {
                        log::trace!("{:?}: Executing task", thread::current().id());
                        (f)();
                    }
                    Ok(Swap(f)) => {
                        // A swap can only occur exactly once.

                        log::trace!(
                            "{:?}: Received a request to swap. Swapping channels",
                            thread::current().id()
                        );

                        // Double-swap:
                        // 1. First put the unbounded receiver in place.
                        // 2. Then drop the old receiver so that all senders will fail.
                        mem::swap(&mut rx, &mut unbounded_rx);
                        let (unused_tx, mut unused_rx) = bounded(0);
                        mem::swap(&mut unbounded_rx, &mut unused_rx);
                        drop(unused_tx);

                        // Finally notify the other side
                        f.send(()).expect("Other side has gone missing");
                    }

                    // Other side was disconnected.
                    Err(e) => {
                        log::trace!(
                            "{:?}: Failed to receive preinit task in worker: Error: {:?}",
                            thread::current().id(),
                            e
                        );
                        return;
                    }
                }
            }
        });

        Dispatcher {
            block_sender: Some(block_sender),
            preinit_sender,
            sender,
            worker: Some(worker),
        }
    }

    pub fn guard(&self) -> DispatchGuard {
        DispatchGuard {
            preinit: self.preinit_sender.clone(),
            sender: self.sender.clone(),
        }
    }

    /// Start processing queued tasks.
    ///
    /// This function blocks until queued tasks prior to this call are finished.
    /// Once the initial queue is empty the dispatcher will wait for new tasks to be launched.
    pub fn flush_init(&mut self) -> Result<(), DispatchError> {
        match self.block_sender.take() {
            Some(tx) => tx.send(())?,
            None => return Err(DispatchError::AlreadyFlushed),
        }

        // Block for queue to empty.
        let (tx, rx) = bounded(1);

        // Closing the pre-init channel.
        // Further messages will be queued on the unbounded queue.
        let (mut sender, unused_rx) = bounded(0);
        mem::swap(&mut self.preinit_sender, &mut sender);
        // Close other side immediately
        drop(unused_rx);

        // Send final (blocking) command.
        sender
            .send(Command::Swap(tx))
            .map_err(|_| DispatchError::SendError)?;

        // Dropping the sender closes the channel
        // and thus the worker will eventually swap to the unbounded queue.
        log::trace!("flush_init. Dropping sender should close the channel.");
        drop(sender);

        rx.recv()?;
        Ok(())
    }

    /// Send a shutdown request to the worker.
    ///
    /// This will initiate a shutdown of the worker thread
    /// and no new tasks will be processed after this.
    /// It will not block on the worker thread.
    ///
    /// The global queue won't be usable after this.
    /// Subsequent calls to `launch` will panic.
    pub fn try_shutdown(&self) -> Result<(), DispatchError> {
        self.guard().shutdown()
    }

    /// Waits for the worker thread to finish and finishes the dispatch queue.
    ///
    /// You need to call `try_shutdown` to initiate a shutdown of the queue.
    pub fn join(mut self) -> Result<(), DispatchError> {
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| DispatchError::WorkerPanic)?;
        }
        Ok(())
    }

    /// Launch a new task on the dispatch queue.
    ///
    /// The new task will be enqueued immediately.
    /// If the queue was already flushed, a background thread will process tasks in the queue (See `flush_init`).
    ///
    /// This will not block.
    pub fn launch(&self, task: impl FnOnce() + Send + 'static) -> Result<(), DispatchError> {
        self.guard().launch(task)
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        if thread::panicking() {
            log::trace!("Thread already panicking. Not blocking on worker.");
        } else {
            log::trace!("Dropping dispatcher, waiting for worker thread.");
            self.worker.take().map(|t| t.join().unwrap());
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::{thread, time::Duration};

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn it_works() {
        init();

        let mut dispatcher = Dispatcher::new(5);
        dispatcher.flush_init().unwrap();

        dispatcher
            .launch(|| {
                // intentionally left empty
            })
            .unwrap();

        dispatcher.try_shutdown().unwrap();
        dispatcher.join().unwrap();
    }

    #[test]
    fn tasks_are_processed_in_order() {
        init();

        let mut dispatcher = Dispatcher::new(10);
        dispatcher.flush_init().unwrap();

        let result = Arc::new(Mutex::new(vec![]));
        for i in 1..=5 {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        dispatcher.try_shutdown().unwrap();
        dispatcher.join().unwrap();

        assert_eq!(&*result.lock().unwrap(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn preinit_tasks_are_processed_after_flush() {
        init();

        let mut dispatcher = Dispatcher::new(10);

        let result = Arc::new(Mutex::new(vec![]));
        for i in 1..=5 {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        result.lock().unwrap().push(0);
        dispatcher.flush_init().unwrap();
        for i in 6..=10 {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        dispatcher.try_shutdown().unwrap();
        dispatcher.join().unwrap();

        assert_eq!(
            &*result.lock().unwrap(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
    }

    #[test]
    fn tasks_after_shutdown_are_not_processed() {
        init();

        let mut dispatcher = Dispatcher::new(10);

        let result = Arc::new(Mutex::new(vec![]));

        dispatcher.flush_init().unwrap();

        dispatcher.try_shutdown().unwrap();
        {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(0);
                })
                .unwrap();
        }

        dispatcher.join().unwrap();

        assert_eq!(&*result.lock().unwrap(), &[]);
    }

    #[test]
    fn preinit_buffer_fills_up() {
        init();

        let mut dispatcher = Dispatcher::new(5);

        let result = Arc::new(Mutex::new(vec![]));

        for i in 1..=5 {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        {
            let result = Arc::clone(&result);
            let err = dispatcher.launch(move || {
                result.lock().unwrap().push(10);
            });
            assert_eq!(Err(DispatchError::QueueFull), err);
        }

        dispatcher.flush_init().unwrap();

        {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(20);
                })
                .unwrap();
        }

        dispatcher.try_shutdown().unwrap();
        dispatcher.join().unwrap();

        assert_eq!(&*result.lock().unwrap(), &[1, 2, 3, 4, 5, 20]);
    }

    #[test]
    fn normal_queue_is_unbounded() {
        init();

        // Note: We can't actually test that it's fully unbounded,
        // but we can quickly queue more slow tasks than the pre-init buffer holds
        // and then guarantuee they all run.

        let mut dispatcher = Dispatcher::new(5);

        let result = Arc::new(Mutex::new(vec![]));

        for i in 1..=5 {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        dispatcher.flush_init().unwrap();

        // Queue more than 5 tasks,
        // Each one is slow to process, so we should be faster in queueing
        // them up than they are processed.
        for i in 6..=20 {
            let result = Arc::clone(&result);
            dispatcher
                .launch(move || {
                    thread::sleep(Duration::from_millis(50));
                    result.lock().unwrap().push(i);
                })
                .unwrap();
        }

        dispatcher.try_shutdown().unwrap();
        dispatcher.join().unwrap();

        let expected = (1..=20).into_iter().collect::<Vec<_>>();
        assert_eq!(&*result.lock().unwrap(), &expected);
    }
}
