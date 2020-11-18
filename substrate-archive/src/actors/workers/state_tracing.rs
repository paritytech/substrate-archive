// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

use sc_tracing::{ProfilingLayer, SpanDatum, TraceEvent, TraceHandler};
use substrate_archive_common::Result;
use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use xtra::prelude::*;

#[derive(Clone)]
struct ArchiveTraceHandler {
	addr: Address<TracingActor>,
}

impl ArchiveTraceHandler {
	fn new(addr: Address<TracingActor>) -> Self {
		Self { addr }
	}
}

impl TraceHandler for ArchiveTraceHandler {
	fn handle_span(&self, sd: SpanDatum) {
		self.addr.do_send(SpanMessage(sd)).unwrap();
	}

	fn handle_event(&self, ev: TraceEvent) {
		self.addr.do_send(EventMessage(ev)).unwrap();
	}
}

pub struct TracingActor {
	targets: String,
}

impl TracingActor {
	pub fn new(targets: String) -> Self {
		TracingActor { targets }
	}
}

#[async_trait::async_trait]
impl Actor for TracingActor {
	async fn started(&mut self, ctx: &mut Context<Self>) {
		log::info!("State Tracing Started");
		let addr = ctx.address().expect("Actor just started");
		let handler = ArchiveTraceHandler::new(addr.clone());
		log::debug!("Trace Targets [{}]", self.targets.as_str());
		let layer = ProfilingLayer::new_with_handler(Box::new(handler), self.targets.as_str());
		let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish().with(layer);
		tracing::subscriber::set_global_default(subscriber).unwrap();
	}
}

#[derive(Debug)]
struct SpanMessage(SpanDatum);

impl Message for SpanMessage {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<SpanMessage> for TracingActor {
	async fn handle(&mut self, msg: SpanMessage, _: &mut Context<Self>) {
		let to_print = msg.0.target.as_str();
		match to_print.as_ref() {
			"sp_io::hashing" | "sp_io::allocator" | "sp_io::storage" => {}
			_ => log::info!("Span: {:?}", to_print),
		}
	}
}

#[derive(Debug)]
struct EventMessage(TraceEvent);

impl Message for EventMessage {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<EventMessage> for TracingActor {
	async fn handle(&mut self, msg: EventMessage, _: &mut Context<Self>) {
		log::info!("Event: {:?}", msg);
	}
}

#[async_trait::async_trait]
impl Handler<super::Die> for TracingActor {
	async fn handle(&mut self, _: super::Die, ctx: &mut Context<Self>) -> Result<()> {
		log::info!("Traces Stopping");
		ctx.stop();
		Ok(())
	}
}
