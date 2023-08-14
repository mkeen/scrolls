use clap::Parser;
use std::process;

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
    let result = match Scrolls::parse() {
        Scrolls::Daemon(x) => daemon::run(&x),
    };

    if let Err(err) = &result {
        eprintln!("ERROR: {:#?}", err);
        process::exit(1);
    }

    Ok(())
}

