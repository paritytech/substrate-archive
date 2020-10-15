use super::error::Result;

pub type KeyValuePair = (Box<[u8]>, Box<[u8]>);

// Archive specific K/V database reader implementation
pub trait ReadOnlyDatabaseTrait: Send + Sync {
    /// Read key/value pairs from the database
    fn get(&self, col: u32, key: &[u8]) -> Option<Vec<u8>>;
    /// Iterate over all blocks in the database
    fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a>;
    /// Catch up with the latest information added to the database
    fn try_catch_up_with_primary(&self) -> Result<()>;
}
