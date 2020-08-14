//! A global dispatcher queue.
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

use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

pub use global::*;

mod global;

/// The command a worker should execute.
enum Command {
    /// A task is a user-defined function to run.
    Task(Box<dyn Fn() + Send>),
    /// Signal the worker to finish work and shut down.
    Shutdown,
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
/// })
/// ```
pub struct Dispatcher {
    preinit_sender: Option<SyncSender<()>>,
    sender: SyncSender<Command>,
    worker: JoinHandle<()>,
}

impl Dispatcher {
    /// Create a new dispatcher with a maximum queue size.
    ///
    /// Launched tasks won't run until `flush_init` is called.
    pub fn new(max_queue_size: usize) -> Self {
        let (preinit_sender, preinit_rx) = mpsc::sync_channel(1);
        let (sender, rx) = mpsc::sync_channel(max_queue_size);

        let worker = thread::spawn(move || {
            log::trace!("Worker started in {:?}", thread::current().id());

            match preinit_rx.recv() {
                Ok(()) => log::trace!(
                    "({:?}) Unblocked. Processing queue.",
                    thread::current().id()
                ),
                Err(e) => {
                    log::error!("({:?}) Failed to receive: {:?}", thread::current().id(), e);
                    return;
                }
            }

            loop {
                use Command::*;

                match rx.recv() {
                    Ok(Shutdown) => {
                        log::trace!("({:?}) Received `Shutdown`", thread::current().id());
                        break;
                    }
                    Ok(Task(f)) => {
                        log::trace!("({:?}) Executing task", thread::current().id());
                        (f)();
                    }

                    Err(e) => {
                        log::error!(
                            "({:?}) Failed to receive task. Error: {:?}",
                            thread::current().id(),
                            e
                        );
                        break;
                    }
                }
            }
        });

        Dispatcher {
            preinit_sender: Some(preinit_sender),
            sender,
            worker,
        }
    }

    /// Start processing queued tasks.
    ///
    /// This function blocks until queued tasks prior to this call are finished.
    /// Once the initial queue is empty the dispatcher will wait for new tasks to be launched.
    pub fn flush_init(&mut self) {
        self.preinit_sender.take().map(|tx| tx.send(()));

        // Block for queue to empty.
        let (tx, rx) = mpsc::channel();

        let task = Box::new(move || {
            log::trace!("End of the queue. Unblock main thread.");
            tx.send(()).unwrap();
        });
        self.sender.send(Command::Task(task)).unwrap();
        rx.recv().unwrap();
    }

    /// Shutdown the dispatch queue.
    ///
    /// This will initiate a shutdown of the worker thread
    /// and block until all enqueued tasks are finished.
    pub fn shutdown(self) {
        let _ = self.sender.try_send(Command::Shutdown);
        self.worker.join().unwrap();
    }

    /// Launch a new task on the dispatch queue.
    ///
    /// The new task will be enqueued immediately.
    /// If the queue was already flushed, a background thread will process tasks in the queue (See `flush_init`).
    ///
    /// This will not block.
    pub fn launch(&mut self, task: impl Fn() + Send + 'static) {
        match self.sender.try_send(Command::Task(Box::new(task))) {
            Ok(()) => (),
            Err(e) => {
                log::error!("Failed to queue new task: {:?}", e);
            }
        }
    }
}
