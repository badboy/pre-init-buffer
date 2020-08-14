use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

pub use global::*;

mod global;

enum Command {
    Task(Box<dyn Fn() + Send>),
    Done,
}

pub struct Dispatcher {
    preinit_sender: Option<SyncSender<()>>,
    sender: SyncSender<Command>,
    worker: Option<JoinHandle<()>>,
}

impl Dispatcher {
    pub fn new(maxqueue: usize) -> Self {
        let (preinit_sender, preinit_rx) = mpsc::sync_channel(1);
        let (sender, rx) = mpsc::sync_channel(maxqueue);

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
                    Ok(Done) => {
                        log::trace!("({:?}) Received `Done`", thread::current().id());
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
            worker: Some(worker),
        }
    }

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

    pub fn finish(&mut self) {
        let _ = self.sender.try_send(Command::Done);
        self.worker.take().map(|w| w.join());
    }

    pub fn launch(&mut self, task: impl Fn() + Send + 'static) {
        match self.sender.try_send(Command::Task(Box::new(task))) {
            Ok(()) => (),
            Err(e) => {
                log::error!("Failed to queue new task: {:?}", e);
            }
        }
    }
}
