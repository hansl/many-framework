mod application;
mod omni_frontend;

use crate::omni_frontend::OmniFrontend;
use clap::Parser;
use omni::Identity;
use omni_abci::application::AbciHttpServer;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tendermint_abci::ClientBuilder;
use tendermint_abci::ServerBuilder;
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, Parser)]
struct Opt {
    /// The interface and port to bind the abci server to.
    #[clap(long, default_value = "127.0.0.1:26658")]
    abci: String,

    /// The default server read buffer size, in bytes, for each incoming client
    /// connection.
    #[clap(short, long, default_value = "1048576")]
    read_buf_size: usize,

    /// Increase output logging verbosity to DEBUG level.
    #[clap(short, long)]
    verbose: bool,

    /// Suppress all output logging (overrides --verbose).
    #[clap(short, long)]
    quiet: bool,

    // OMNI Protocol Host interface and port to listen to.
    #[clap(long, default_value = "127.0.0.1:8000")]
    omni: String,

    // OMNI PEM file for the identity.
    #[clap(long)]
    pem: PathBuf,
}

fn main() {
    let opt: Opt = Opt::parse();
    let log_level = if opt.quiet {
        LevelFilter::OFF
    } else if opt.verbose {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();

    let (app, driver) = application::KeyValueStoreApp::new();
    let abci_server = ServerBuilder::new(opt.read_buf_size)
        .bind(opt.abci.clone(), app)
        .unwrap();

    let abci_client = Arc::new(Mutex::new(
        ClientBuilder::default().connect(opt.abci.clone()).unwrap(),
    ));
    let bytes = std::fs::read(opt.pem).unwrap();
    let (id, keypair) = Identity::from_pem_addressable(bytes).unwrap();

    let omni_server = omni::transport::http::HttpServer::new(AbciHttpServer::new(
        abci_client,
        OmniFrontend {},
        id,
        Some(keypair),
    ));

    let omni = opt.omni.clone();
    let j1 = std::thread::spawn(move || driver.run().unwrap());
    let j2 = std::thread::spawn(move || omni_server.bind(omni).unwrap());
    let j3 = std::thread::spawn(move || abci_server.listen().unwrap());

    j1.join().unwrap();
    j2.join().unwrap();
    j3.join().unwrap();
}
