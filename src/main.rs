use std::sync::mpsc::{self, Sender, SyncSender};
use std::thread::{self, JoinHandle};

enum Command {
    Task(Box<dyn Fn() + Send>),
    Done,
}

struct Dispatcher {
    preinit_sender: Option<Sender<()>>,
    sender: SyncSender<Command>,
    worker: Option<JoinHandle<()>>,
}

impl Dispatcher {
    fn new(maxqueue: usize) -> Self {
        let (preinit_sender, preinit_rx) = mpsc::channel();
        let (sender, rx) = mpsc::sync_channel(maxqueue);

        let worker = thread::spawn(move || {
            println!("Worker started in {:?}", thread::current().id());

            match preinit_rx.recv() {
                Ok(()) => println!("({:?}) Unblocked. Processing queue.", thread::current().id()),
                Err(e) => {
                    println!("({:?}) Failed to receive: {:?}", thread::current().id(), e);
                    return;
                }
            }

            loop {
                use Command::*;

                match rx.recv() {
                    Ok(Done) => {
                        println!("({:?}) Received `Done`", thread::current().id());
                        break;
                    }
                    Ok(Task(f)) => {
                        println!("({:?}) Executing task", thread::current().id());
                        (f)();
                    }

                    Err(e) => {
                        println!("({:?}) Failed to receive task. Error: {:?}", thread::current().id(), e);
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

    fn flush_init(&mut self) {
        self.preinit_sender.take().map(|tx| tx.send(()));

        // Block for queue to empty.
        let (tx, rx) = mpsc::channel();

        let task = Box::new(move || {
            println!("End of the queue. Unblock main thread.");
            tx.send(()).unwrap();
        });
        self.sender.send(Command::Task(task)).unwrap();
        rx.recv().unwrap();
    }

    fn finish(&mut self) {
        let _ = self.sender.try_send(Command::Done);
        self.worker.take().map(|w| w.join());
    }

    fn launch(&mut self, task: impl Fn() + Send + 'static) {
        match self.sender.try_send(Command::Task(Box::new(task))) {
            Ok(()) => (),
            Err(e) => {
                println!("Failed to queue new task: {:?}", e);
            }
        }
    }
}

fn main() {
    let mut dispatcher = Dispatcher::new(10);

    println!("Enqueuing.");
    for i in 1..=11 {
        println!("Enqueue {}", i);
        dispatcher.launch(move || {
            println!("Executing task {}", i);
        })
    }

    println!("Flushing.");
    dispatcher.flush_init();

    dispatcher.launch(move || {
        println!("Another task!");
    });

    println!("Finishing.");
    dispatcher.finish();

    println!("Done");
}
