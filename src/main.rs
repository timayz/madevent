use clap::{arg, Command};
use std::str::FromStr;
use tracing::error;
use tracing_subscriber::{prelude::*, EnvFilter};

#[tokio::main]
async fn main() {
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg_required_else_help(true)
        .arg(arg!(--log <LOG>).default_value("error"))
        .subcommand(Command::new("serve").arg(arg!(--config <CONFIG>)))
        .get_matches();

    let log = matches
        .get_one::<String>("log")
        .map(|v| v.as_str())
        .expect("defaulted in clap");

    let Ok(env_filter) = EnvFilter::from_str(log) else {
        println!("invalid value for log arg");
        std::process::exit(1);
    };

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .init();

    match matches.subcommand() {
        Some(("serve", _sub_matches)) => {
            error!("{log}");
        }
        _ => unreachable!(),
    }
}
