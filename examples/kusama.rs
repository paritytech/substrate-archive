use desub::decoder::Decoder;
use desub_extras::polkadot::PolkadotTypes;
use substrate_archive::Archive;
use subxt::KusamaRuntime;

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Debug);
    let types = PolkadotTypes::new().unwrap();
    let decoder = Decoder::new(types, "kusama");
    log::info!("Creating client");
    let client = async_std::task::block_on(
        subxt::ClientBuilder::<KusamaRuntime>::new()
            .set_url("ws://127.0.0.1:9944")
            .build(),
    )
    .unwrap();

    log::info!("Instantiating archive...");
    let archive = Archive::new(decoder, client).unwrap();
    log::info!("Beginning to crawl for info");

    let (data, blocks, blocks_batch, batch_handler) = archive.parts().unwrap();
    async_std::task::spawn(blocks);
    async_std::task::spawn(async {
        match blocks_batch.await {
            Ok(_) => (),
            Err(e) => {
                log::error!("{:?}", e);
                panic!("internal archive error");
            }
        }
    });
    let batch_handler = async {
        match batch_handler.await {
            Ok(_) => (),
            Err(e) => {
                log::error!("{:?}", e);
                panic!("internal archive error");
            }
        }
    };
    let handlers = futures::future::join(batch_handler, data);

    async_std::task::block_on(handlers);
}
