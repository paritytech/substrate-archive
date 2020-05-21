// Copyright 2019-2020 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

//! Storage API backend
//! Largely taken from `substrate/client/rpc/src/state/state_full.rs`
//! (Not using RPC because query_storage calls will hang RPC for any other calls that are made to it)

use super::ChainAccess;
use crate::{
    error::Error as ArchiveError,
    types::{NotSignedBlock, Substrate},
};
use primitive_types::H256;
use sc_client_api::StorageProvider;
use sp_blockchain::{CachedHeaderMetadata, Error as ClientError, HeaderBackend, HeaderMetadata};
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, CheckedSub, NumberFor, SaturatedConversion},
};
use sp_storage::{StorageChangeSet, StorageData, StorageKey};
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

pub struct StorageBackend<T: Substrate, C: ChainAccess<NotSignedBlock>> {
    client: Arc<C>,
    _marker: PhantomData<T>,
}

/// Ranges to query in state_queryStorage.
struct QueryStorageRange<Block: BlockT> {
    /// Hashes of all the blocks in the range.
    pub hashes: Vec<Block::Hash>,
    /// Number of the first block in the range.
    pub first_number: NumberFor<Block>,
    /// Blocks subrange ([begin; end) indices within `hashes`) where we should read keys at
    /// each state to get changes.
    pub unfiltered_range: Range<usize>,
    /// Blocks subrange ([begin; end) indices within `hashes`) where we could pre-filter
    /// blocks-with-changes by using changes tries.
    pub filtered_range: Option<Range<usize>>,
}

impl<T, C> StorageBackend<T, C>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock>,
{
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            _marker: PhantomData,
        }
    }

    /// Returns given block hash or best block hash if None is passed.
    fn block_or_best(&self, hash: Option<H256>) -> Result<H256, ArchiveError> {
        Ok(hash.unwrap_or_else(|| self.client.info().best_hash))
    }

    fn query_storage_unfiltered(
        &self,
        range: &QueryStorageRange<NotSignedBlock>,
        keys: &[StorageKey],
        last_values: &mut HashMap<StorageKey, Option<StorageData>>,
        changes: &mut Vec<StorageChangeSet<H256>>,
    ) -> Result<(), ArchiveError> {
        for block in range.unfiltered_range.start..range.unfiltered_range.end {
            let block_hash = range.hashes[block].clone();
            let mut block_changes = StorageChangeSet {
                block: block_hash.clone(),
                changes: Vec::new(),
            };
            let id = BlockId::hash(block_hash);
            for key in keys {
                let (has_changed, data) = {
                    let curr_data = self.client.storage(&id, key)?;
                    match last_values.get(key) {
                        Some(prev_data) => (curr_data != *prev_data, curr_data),
                        None => (true, curr_data),
                    }
                };
                if has_changed {
                    block_changes.changes.push((key.clone(), data.clone()));
                }
                last_values.insert(key.clone(), data);
            }
            if !block_changes.changes.is_empty() {
                changes.push(block_changes);
            }
        }
        Ok(())
    }

    /// Iterates through all blocks that are changing keys within range.filtered_range and collects these changes.
    fn query_storage_filtered(
        &self,
        range: &QueryStorageRange<NotSignedBlock>,
        keys: &[StorageKey],
        last_values: &HashMap<StorageKey, Option<StorageData>>,
        changes: &mut Vec<StorageChangeSet<H256>>,
    ) -> Result<(), ArchiveError> {
        let (begin, end) = match range.filtered_range {
            Some(ref filtered_range) => (
                range.first_number + filtered_range.start.saturated_into::<u32>(),
                BlockId::Hash(range.hashes[filtered_range.end - 1].clone()),
            ),
            None => return Ok(()),
        };
        let mut changes_map: BTreeMap<NumberFor<NotSignedBlock>, StorageChangeSet<H256>> =
            BTreeMap::new();
        for key in keys {
            let mut last_block = None;
            let mut last_value = last_values.get(key).cloned().unwrap_or_default();
            let key_changes = self
                .client
                .key_changes(begin, end, None, key)
                .map_err(ArchiveError::from)?;
            for (block, _) in key_changes.into_iter().rev() {
                if last_block == Some(block) {
                    continue;
                }

                let block_hash =
                    range.hashes[(block - range.first_number).saturated_into::<usize>()].clone();
                let id = BlockId::Hash(block_hash);
                let value_at_block = self.client.storage(&id, key)?;
                if last_value == value_at_block {
                    continue;
                }

                changes_map
                    .entry(block)
                    .or_insert_with(|| StorageChangeSet {
                        block: block_hash,
                        changes: Vec::new(),
                    })
                    .changes
                    .push((key.clone(), value_at_block.clone()));
                last_block = Some(block);
                last_value = value_at_block;
            }
        }
        if let Some(additional_capacity) = changes_map.len().checked_sub(changes.len()) {
            changes.reserve(additional_capacity);
        }
        changes.extend(changes_map.into_iter().map(|(_, cs)| cs));
        Ok(())
    }

    /// Splits the `query_storage` block range into 'filtered' and 'unfiltered' subranges.
    /// Blocks that contain changes within filtered subrange could be filtered using changes tries.
    /// Blocks that contain changes within unfiltered subrange must be filtered manually.
    fn split_query_storage_range(
        &self,
        from: H256,
        to: Option<H256>,
    ) -> Result<QueryStorageRange<NotSignedBlock>, ArchiveError> {
        let to = self.block_or_best(to)?;

        let from_meta = self
            .client
            .header_metadata(H256::from_slice(from.as_ref()))?;
        let to_meta = self.client.header_metadata(H256::from_slice(to.as_ref()))?;

        if from_meta.number > to_meta.number {
            return Err(invalid_block_range(
                &from_meta,
                &to_meta,
                "from number > to number".to_owned(),
            ));
        }

        // check if we can get from `to` to `from` by going through parent_hashes.
        let from_number = from_meta.number;
        let hashes = {
            let mut hashes = vec![to_meta.hash];
            let mut last = to_meta.clone();
            while last.number > from_number {
                let header_metadata = self.client.header_metadata(last.parent).map_err(|e| {
                    invalid_block_range::<NotSignedBlock>(&last, &to_meta, e.to_string())
                })?;
                hashes.push(header_metadata.hash);
                last = header_metadata;
            }
            if last.hash != from_meta.hash {
                return Err(invalid_block_range(
                    &from_meta,
                    &to_meta,
                    "from and to are on different forks".to_owned(),
                ));
            }
            hashes.reverse();
            hashes
        };

        // check if we can filter blocks-with-changes from some (sub)range using changes tries
        let changes_trie_range = self
            .client
            .max_key_changes_range(from_number, BlockId::Hash(to_meta.hash))?;
        let filtered_range_begin = changes_trie_range.and_then(|(begin, _)| {
            // avoids a corner case where begin < from_number (happens when querying genesis)
            begin
                .checked_sub(from_number)
                .map(|x| x.saturated_into::<usize>())
        });
        let (unfiltered_range, filtered_range) = split_range(hashes.len(), filtered_range_begin);

        Ok(QueryStorageRange {
            hashes,
            first_number: from_number,
            unfiltered_range,
            filtered_range,
        })
    }

    fn storage_keys(
        &self,
        block: Option<T::Hash>,
        prefix: StorageKey,
    ) -> Result<Vec<StorageKey>, ArchiveError> {
        let block = self.block_or_best(block.map(|h| H256::from_slice(h.as_ref())))?;
        self.client
            .storage_keys(&BlockId::Hash(block), &prefix)
            .map_err(Into::into)
    }

    pub fn query_storage(
        &self,
        from: T::Hash,
        to: Option<T::Hash>,
        keys: Vec<StorageKey>,
    ) -> Result<Vec<StorageChangeSet<H256>>, ArchiveError> {
        let mut full_keys = Vec::new();
        log::info!(
            "Prefixes: {:?}",
            keys.iter()
                .map(|k| hex::encode(k.0.as_slice()))
                .collect::<Vec<String>>()
        );
        for prefix in keys.into_iter() {
            full_keys.extend(self.storage_keys(Some(from), prefix)?.into_iter())
        }
        log::info!(
            "Full Keys: {:?}",
            full_keys
                .iter()
                .map(|k| hex::encode(k.0.as_slice()))
                .collect::<Vec<String>>()
        );
        let keys = full_keys;
        let range = self.split_query_storage_range(
            H256::from_slice(from.as_ref()),
            to.map(|h| H256::from_slice(h.as_ref())),
        )?;
        let mut changes = Vec::new();
        let mut last_values = std::collections::HashMap::new();
        self.query_storage_unfiltered(&range, &keys, &mut last_values, &mut changes)?;
        self.query_storage_filtered(&range, &keys, &last_values, &mut changes)?;
        Ok(changes)
    }
}

/// Splits passed range into two subranges where:
/// - first range has at least one element in it;
/// - second range (optionally) starts at given `middle` element.
fn split_range(size: usize, middle: Option<usize>) -> (Range<usize>, Option<Range<usize>>) {
    // check if we can filter blocks-with-changes from some (sub)range using changes tries
    let range2_begin = match middle {
        // some of required changes tries are pruned => use available tries
        Some(middle) if middle != 0 => Some(middle),
        // all required changes tries are available, but we still want values at first block
        // => do 'unfiltered' read for the first block and 'filtered' for the rest
        Some(_) if size > 1 => Some(1),
        // range contains single element => do not use changes tries
        Some(_) => None,
        // changes tries are not available => do 'unfiltered' read for the whole range
        None => None,
    };
    let range1 = 0..range2_begin.unwrap_or(size);
    let range2 = range2_begin.map(|begin| begin..size);
    (range1, range2)
}

fn invalid_block_range<B: BlockT>(
    from: &CachedHeaderMetadata<B>,
    to: &CachedHeaderMetadata<B>,
    details: String,
) -> ArchiveError {
    let to_string = |h: &CachedHeaderMetadata<B>| format!("{} ({:?})", h.number, h.hash);

    ArchiveError::InvalidBlockRange {
        from: to_string(from),
        to: to_string(to),
        details,
    }
}

fn invalid_block<B: BlockT>(from: H256, to: Option<H256>, details: String) -> ArchiveError {
    ArchiveError::InvalidBlockRange {
        from: format!("{:?}", from),
        to: format!("{:?}", to),
        details,
    }
}
