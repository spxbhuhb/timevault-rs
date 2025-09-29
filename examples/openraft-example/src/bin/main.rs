use clap::Parser;
use openraft_example::start_example_raft_node;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long, default_value = "1")]
    pub id: u64,

    #[clap(long, default_value = "127.0.0.1:21000")]
    pub http_addr: String,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    start_example_raft_node(options.id, "./var", options.http_addr).await
}
