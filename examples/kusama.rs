use desub::decoder::Decoder;
use desub_extras::polkadot::PolkadotTypes;
use subxt::{KusamaRuntime, system::System};
use substrate_archive::Archive;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let types = PolkadotTypes::new().unwrap();
    let decoder = Decoder::new(types, "kusama");
    log::info!("Creating client");
    let client = async_std::task::block_on(subxt::ClientBuilder::<KusamaRuntime>::new().set_url("ws://127.0.0.1:9944").build()).unwrap();
    
    let block = async_std::task::block_on(client.block::<<KusamaRuntime as System>::Hash>(None)).unwrap();
    println!("{:?}", block);

    log::info!("Instantiating archive..."); 
    let archive = Archive::new(decoder, client).unwrap();
    log::info!("Beginning to crawl for info");
    let (data, blocks, blocks_batch, batch_handler) = archive.parts().unwrap();
    async_std::task::spawn(blocks);
    async_std::task::block_on(data);
}


