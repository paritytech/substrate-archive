//! A simple example

use polkadot_service::kusama_runtime as ksm_runtime;
use sc_service::config::DatabaseConfig; // integrate this into Archive Proper
use sp_blockchain::HeaderBackend as _;
use sp_storage::StorageKey;
use substrate_archive::{backend, init, NotSignedBlock};
use sp_core::twox_128;

type Block = NotSignedBlock<ksm_runtime::Runtime>;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Warn, log::LevelFilter::Info);

    // FIXME Database and spec initialization can be done in the lib with a convenience func
    let db = backend::open_database::<Block>(
        "/home/insipx/.local/share/polkadot/chains/ksmcc3/db",
        4096,
    )
    .unwrap();
    let conf = DatabaseConfig::Custom(db);

    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let client = backend::client::<
        ksm_runtime::Runtime,
        ksm_runtime::RuntimeApi,
        polkadot_service::KusamaExecutor,
        _,
    >(conf, spec)
    .unwrap();

    let info = client.info();
    println!("{:?}", info);

    // TODO: create a 'key builder' for key prefixes
    // this may already exist in substrate (haven't checked)
    let system_key = twox_128(b"System").to_vec();
    let events_key = twox_128(b"Events").to_vec();
    let accounts_key = twox_128(b"Account").to_vec();

    let democracy_key = twox_128(b"Democracy").to_vec();
    let public_proposal_count = twox_128(b"PublicPropCount").to_vec();
    let public_proposals = twox_128(b"PublicProps").to_vec();


    let mut keys = Vec::new();

    let mut system_accounts = system_key.clone();
    system_accounts.extend(accounts_key);
    let mut system_events = system_key.clone();
    system_events.extend(events_key);
    let mut democracy_public_proposal_count = democracy_key.clone();
    democracy_public_proposal_count.extend(public_proposal_count);
    let mut democracy_proposals = democracy_key.clone();
    democracy_proposals.extend(public_proposals);

    keys.push(StorageKey(system_accounts));
    keys.push(StorageKey(system_events));
    keys.push(StorageKey(democracy_public_proposal_count));
    keys.push(StorageKey(democracy_proposals));

    init::<ksm_runtime::Runtime, _>(client, "ws://127.0.0.1:9944".to_string(), keys).unwrap();
}
