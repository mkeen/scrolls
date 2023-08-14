use clap::Parser;
use std::process;

use tokio::signal;
use tokio::sync::mpsc;

mod console;
mod daemon;

#[derive(Parser)]
#[clap(name = "Scrolls")]
#[clap(bin_name = "scrolls")]
#[clap(author, version, about, long_about = None)]
enum Scrolls {
    Daemon(daemon::Args),
}

async fn random() {
    println!("hi there")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    let _ = random();
                },
            }
        }
    });

    let result = match Scrolls::parse() {
        Scrolls::Daemon(x) => daemon::run(&x),
    };





    // if let Err(err) = &result {
    //     eprintln!("ERROR: {:#?}", err);
    //     process::exit(1);
    // }

    Ok(())
}

