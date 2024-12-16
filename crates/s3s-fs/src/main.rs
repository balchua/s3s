#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(clippy::needless_return)]

use futures::future::BoxFuture;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use s3s::Body;
use s3s_fs::FileSystem;
use s3s_fs::Result;

use s3s::auth::SimpleAuth;
use s3s::host::MultiDomain;
use s3s::service::S3ServiceBuilder;
use tower::Layer;
use tower::Service;
use tower_http::cors::CorsLayer;

use std::io::IsTerminal;
use std::ops::Not;
use std::path::PathBuf;
use std::task::Context;
use std::task::Poll;

use tokio::net::TcpListener;

use clap::{CommandFactory, Parser};
use tracing::info;

use futures::future::ready;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::service::TowerToHyperService;
use s3s::S3Error;

#[derive(Debug, Parser)]
#[command(version)]
struct Opt {
    /// Host name to listen on.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Port number to listen on.
    #[arg(long, default_value = "8014")] // The original design was finished on 2020-08-14.
    port: u16,

    /// Access key used for authentication.
    #[arg(long)]
    access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long)]
    secret_key: Option<String>,

    /// Domain names used for virtual-hosted-style requests.
    #[arg(long)]
    domain: Vec<String>,

    /// Root directory of stored data.
    root: PathBuf,
}

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::from_default_env();
    let enable_color = std::io::stdout().is_terminal();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}

fn check_cli_args(opt: &Opt) {
    use clap::error::ErrorKind;

    let mut cmd = Opt::command();

    // TODO: how to specify the requirements with clap derive API?
    if let (Some(_), None) | (None, Some(_)) = (&opt.access_key, &opt.secret_key) {
        let msg = "access key and secret key must be specified together";
        cmd.error(ErrorKind::MissingRequiredArgument, msg).exit();
    }

    for s in &opt.domain {
        if s.contains('/') {
            let msg = format!("expected domain name, found URL-like string: {s:?}");
            cmd.error(ErrorKind::InvalidValue, msg).exit();
        }
    }
}

fn main() -> Result {
    let opt = Opt::parse();
    check_cli_args(&opt);

    setup_tracing();

    run(opt)
}

#[tokio::main]
async fn run(opt: Opt) -> Result {
    // Setup S3 provider
    let fs = FileSystem::new(opt.root)?;

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(fs);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            b.set_auth(SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        // Enable parsing virtual-hosted-style requests
        if opt.domain.is_empty().not() {
            b.set_host(MultiDomain::new(&opt.domain)?);
            info!("virtual-hosted-style requests are enabled");
        }

        b.build()
    };

    // Run server
    let listener = TcpListener::bind((opt.host.as_str(), opt.port)).await?;
    let local_addr = listener.local_addr()?;

    let hyper_service = service.into_shared();

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("server is running at http://{local_addr}");

    loop {
        let (socket, _) = tokio::select! {
            res =  listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };

        let io = TokioIo::new(socket);

        let conn = http_server.serve_connection(
            io,
            TowerToHyperService::new(
                tower::ServiceBuilder::new()
                    .layer(BodyLimitLayer::new(1024 * 1024 * 5))
                    // .layer(ConcurrencyLimitLayer::new(2))
                    .service(hyper_service.clone()),
            ),
        );
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    tokio::select! {
        () = graceful.shutdown() => {
             tracing::debug!("Gracefully shutdown!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
             tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
        }
    }

    info!("server is stopped");
    Ok(())
}

#[derive(Clone)]
struct BodyLimitLayer {
    limit: usize,
}

impl BodyLimitLayer {
    fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl<S> Layer<S> for BodyLimitLayer {
    type Service = BodyLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BodyLimit {
            inner,
            limit: self.limit,
        }
    }
}

#[derive(Clone)]
struct BodyLimit<S> {
    inner: S,
    limit: usize,
}

impl<S> BodyLimit<S> {
    fn new(inner: S, limit: usize) -> Self {
        Self { inner, limit }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for BodyLimit<S>
where
    S: Service<Request<ReqBody>, Response = Response<Body>, Error = S3Error> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: hyper::body::Body + Send + 'static,
    ReqBody::Data: Send,
{
    type Response = Response<Body>;
    type Error = S3Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if let Some(content_length) = req.headers().get(hyper::header::CONTENT_LENGTH) {
            if let Ok(content_length) = content_length.to_str().and_then(|s| Ok(s.parse::<usize>().map_err(|_| ()))) {
                if content_length > Ok(self.limit) {
                    let response = Response::builder()
                        .status(StatusCode::PAYLOAD_TOO_LARGE)
                        .body(Body::from(b"Payload too large".to_vec()))
                        .unwrap();
                    return Box::pin(ready(Ok(response)));
                }
            }
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}
