// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use eframe::{egui, epi};
use profiling::puffin;
use anyhow::Result;
use substrate_archive::{Archive, ArchiveConfig, ReadOnlyDb};
use polkadot_service::Block;

pub struct PolkadotArchive<D: ReadOnlyDb + 'static> {
    archive: Box<dyn Archive<Block, D>>
}


impl<D: ReadOnlyDb + 'static> PolkadotArchive<D> {
    pub fn new(chain_spec: &str, config: Option<ArchiveConfig>) -> Result<Self> {
        let mut archive = super::run_archive(chain_spec, config)?;
        archive.drive()?;
        Ok(Self { archive })
    }
}

impl<D: ReadOnlyDb + 'static> epi::App for PolkadotArchive<D> {
    fn name(&self) -> &str {
        "profiling egui eframe for Polkadot Archive"     
    }
    
    fn update(&mut self, ctx: &egui::CtxRef, _frame: &mut epi::Frame<'_>) {
        puffin_egui::profiler_window(ctx);        
        puffin::GlobalProfiler::lock().new_frame();
    }

    fn on_exit(&mut self) {
       if let Err(e) = self.archive.shutdown() {
            log::error!("{:?}", e);
       }
    }
}
