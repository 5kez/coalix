//! coalix binary — a thin CLI shell around the engine crate.
//!
//! Besides the configuration preflight (--check / --print-config), every
//! invocation now starts the hyper reverse-proxy loop and runs until
//! ctrl-c; the CLI surface is unchanged since phase 1.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;

use coalix::config::{Config, LogFormat, ObservabilityConfig};

/// Coalix — zero-config request-coalescing reverse proxy.
#[derive(Debug, Parser)]
#[command(name = "coalix", version, about, long_about = None)]
struct Cli {
    /// Path to a YAML configuration file (zero-config defaults when omitted)
    #[arg(short = 'c', long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Validate the configuration and exit non-zero on failure
    #[arg(long)]
    check: bool,

    /// Print the fully resolved configuration as YAML and exit
    #[arg(long)]
    print_config: bool,
}

/// Installs the tracing subscriber from the resolved observability block.
///
/// `RUST_LOG` still wins for compatibility; otherwise
/// `observability.log_level` scopes the `coalix` target and
/// `observability.log_format` picks human-readable or JSON encoding.
/// Already-initialized subscribers are ignored, so this stays safe to call
/// once from every entry point and test harness.
fn init_tracing(observability: &ObservabilityConfig) {
    let filter =
        std::env::var("RUST_LOG").unwrap_or_else(|_| format!("coalix={}", observability.log_level));
    let builder =
        tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(filter));
    let result = match observability.log_format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
    if let Err(err) = result {
        // Tests install their own subscriber first; that is the usual case.
        tracing::debug!(%err, "tracing subscriber already installed");
    }
}

/// Starts the tokio runtime, loads configuration, dispatches CLI modes,
/// and otherwise serves until the shutdown signal.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = Arc::new(
        Config::load(cli.config.as_deref())
            .context("configuration invalid - run: coalix --check --config FILE")?,
    );
    // Observability follows the resolved configuration: RUST_LOG or
    // COALIX_LOG_LEVEL for severity, log_format for text vs. JSON.
    init_tracing(&config.observability);

    if cli.print_config {
        print!("{}", config.to_yaml()?);
        return Ok(());
    }

    if cli.check {
        println!(
            "coalix: configuration OK - listen {}, {} route rule(s)",
            config.server.listen,
            config.routes.len()
        );
        return Ok(());
    }

    tracing::info!(
        listen = %config.server.listen,
        upstream = %config.upstream.base_url,
        coalescing = config.coalescing.enabled,
        "starting coalix"
    );
    println!(
        "coalix {} - listening on {} -> {} (coalescing {}, {} route rule(s))",
        coalix::VERSION,
        config.server.listen,
        config.upstream.base_url,
        if config.coalescing.enabled {
            "on"
        } else {
            "off"
        },
        config.routes.len()
    );
    coalix::proxy::serve(config)
        .await
        .context("proxy loop terminated")?;
    Ok(())
}
