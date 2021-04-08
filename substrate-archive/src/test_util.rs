use anyhow::Error;
use std::{
    fs::File,
    convert::TryInto
};

#[derive(Debug)]
struct CsvBlock<'a> {
    parent_hash: Vec<u8>,
    block_num: usize,
    state_root: Vec<u8>,
    extrinsics_root: Vec<u8>,
    digest: Vec<u8>,
    // scale encoded ext
    ext: Vec<u8>,
    spec: usize
}

impl<'a> CsvBlock<'a> {
    fn extrinsics(&'a self) -> impl ExactSizeIterator<Item = impl AsRef<[u8]> + Clone> + Clone {
        let ext: Vec<Vec<u8>> = Decode::decode(&mut self.ext.as_slice()).unwrap();
        ext.into_iter()
    }
}

fn get_blocks<'a>() -> Result<Vec<CsvBlock<'a>>, Error> {
    let blocks = File::open("test/10K_ksm_blocks.csv")?;
    let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b'\t').from_reader(blocks);
    let mut blocks = Vec::new();
    for line in rdr.records() {
        let line = line?;
        let parent_hash: Vec<u8> = hex::decode(line.get(1).unwrap().strip_prefix("\\\\x").unwrap())?;
        let block_num: usize = line.get(3).unwrap().parse()?;
        let state_root: Vec<u8> = hex::decode(line.get(4).unwrap().strip_prefix("\\\\x").unwrap())?;
        let extrinsics_root: Vec<u8> = hex::decode(line.get(5).unwrap().strip_prefix("\\\\x").unwrap())?;
        let digest = DigestRef::empty();

        let ext: Vec<u8> = hex::decode(line.get(7).unwrap().strip_prefix("\\\\x").unwrap())?;
        let spec: usize = line.get(8).unwrap().parse()?;
        let block = CsvBlock { parent_hash, block_num, state_root, extrinsics_root, digest, ext, spec };
        blocks.push(block);
    }
    Ok(blocks)
}
