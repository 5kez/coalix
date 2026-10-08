//! Accept loop: binds the listener, caps connections, drives HTTP/1.

use std::convert::Infallible;
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::coalescer::Coalescer;
use crate::config::Config;

use super::handler::Handler;
use super::ProxyError;
use super::Upstream;

/// Binds `server.listen` and serves until ctrl-c or a fatal error.
pub async fn serve(config: Arc<Config>) -> Result<(), ProxyError> {
    let addr = config.server.listen;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| ProxyError::Bind { addr, source })?;
    serve_with_listener(listener, config).await
}

/// Serves on an already-bound listener — tests bind ephemeral ports this
/// way without touching the configured address.
pub async fn serve_with_listener(
    listener: TcpListener,
    config: Arc<Config>,
) -> Result<(), ProxyError> {
    let bound = listener.local_addr().map_err(|source| ProxyError::Bind {
        addr: config.server.listen,
        source,
    })?;
    let upstream = Arc::new(Upstream::new(&config.upstream)?);
    let handler = Arc::new(Handler::new(
        config.clone(),
        upstream,
        Arc::new(Coalescer::new(&config.coalescing)),
    ));
    let connection_limit = Arc::new(Semaphore::new(config.server.max_connections));
    info!(
        %bound,
        upstream = %config.upstream.base_url,
        "coalix proxy listening"
    );

    tokio::select! {
        () = accept_loop(&listener, handler, connection_limit, config.server.tcp_nodelay) => {}
        signal = tokio::signal::ctrl_c() => match signal {
            Ok(()) => info!("shutdown signal received"),
            Err(err) => warn!(%err, "signal listener failed; shutting down anyway"),
        },
    }
    Ok(())
}

/// Accepts forever, handing each connection a permit held until its task
/// ends — so `max_connections` bounds live connections, keep-alive included.
async fn accept_loop(
    listener: &TcpListener,
    handler: Arc<Handler>,
    limit: Arc<Semaphore>,
    tcp_nodelay: bool,
) {
    loop {
        let permit = match limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!("connection semaphore closed");
                return;
            }
        };
        // Transient accept failures (EMFILE, interrupted) must not kill the
        // proxy: back off briefly and keep listening.
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                warn!(%err, "accept failed; retrying in 50 ms");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        if let Err(err) = stream.set_nodelay(tcp_nodelay) {
            debug!(%peer, %err, "could not set TCP_NODELAY");
        }

        let handler = handler.clone();
        let connection = http1::Builder::new().serve_connection(
            TokioIo::new(stream),
            service_fn(move |request| {
                let handler = handler.clone();
                async move { Ok::<_, Infallible>(handler.call(request).await) }
            }),
        );
        // Detach the task; the permit moves in and drops with the
        // connection, releasing the slot. `drop` is the idiomatic detach
        // (the JoinHandle is intentionally unused, but must stay alive in
        // flight).
        std::mem::drop(tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = connection.await {
                // Client resets and protocol garbage are routine at scale.
                debug!(%peer, %err, "connection closed with error");
            }
        }));
    }
}
