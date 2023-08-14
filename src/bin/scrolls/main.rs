use clap::Parser;
use std::process;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod console;
mod daemon;

#[derive(Parser)]
#[clap(name = "Scrolls")]
#[clap(bin_name = "scrolls")]
#[clap(author, version, about, long_about = None)]
enum Scrolls {
    Daemon(daemon::Args),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = CancellationToken::new();
    let token_daemon = token.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    token.cancel();
                },
            }
        }
    });

    let m = match Scrolls::parse() {
        Scrolls::Daemon(x) => daemon::run(&x, token_daemon),
    };

    // if let Err(err) = &result {
    //     eprintln!("ERROR: {:#?}", err);
    //     process::exit(1);
    // }

    Ok(())
}

