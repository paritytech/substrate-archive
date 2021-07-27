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
use substrate_archive::{Archive, ArchiveConfig, ReadOnlyDb};
use polkadot_service::Block;


pub struct Profiler<D: ReadOnlyDb + 'static> {
    archive: Box<dyn Archive<Block, D>>
}

impl<D: ReadOnlyDb + 'static> Profiler<D> {
    pub fn new(archive: Box<dyn Archive<Block, D>>) -> Self {
        Self { archive }
    }
}


impl<D: ReadOnlyDb + 'static> epi::App for Profiler<D> {
    fn name(&self) -> &str {
        "profiling egui eframe for Polkadot Archive"     
    }
    
    fn update(&mut self, ctx: &egui::CtxRef, _frame: &mut epi::Frame<'_>) {
        puffin_egui::profiler_window(ctx);        
    }
    
    fn on_exit(&mut self) {
        if let Err(e) = self.archive.shutdown() {
            log::error!("{:?}", e); 
        }
    }
}
