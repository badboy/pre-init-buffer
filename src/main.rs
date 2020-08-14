fn main() {
    env_logger::init();

    println!("Enqueuing.");
    for i in 1..=11 {
        println!("Enqueue {}", i);
        dispatcher::launch(move || {
            println!("Executing task {}", i);
        })
    }

    println!("Flushing.");
    dispatcher::flush_init();

    dispatcher::launch(move || {
        println!("Another task!");
    });

    println!("Finishing.");
    dispatcher::finish();

    println!("Done");
}
