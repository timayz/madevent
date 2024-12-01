use clap::{arg, Parser, Subcommand};
use std::str::FromStr;
use tracing::info;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(arg_required_else_help = true)]
struct Cli {
    #[arg(long, default_value = "error")]
    log: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Serve { config: Option<String> },
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let Ok(env_filter) = EnvFilter::from_str(&args.log) else {
        println!("invalid value for log arg");
        std::process::exit(1);
    };

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .init();

    match args.command {
        Commands::Serve { config } => {
            info!("{config:?}");
        }
    }
}
