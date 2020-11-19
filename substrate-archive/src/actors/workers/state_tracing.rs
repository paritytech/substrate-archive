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

use substrate_archive_common::Result;
use tracing::{
	event::Event,
	span::{Attributes, Id, Record},
	Level, Metadata, Subscriber,
};
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

impl Subscriber for ArchiveTraceHandler {
	fn enabled(&self, metadata: &Metadata<'_>) -> bool {
		log::info!("{}", metadata.target());
		true
	}

	fn new_span(&self, span: &Attributes<'_>) -> Id {
		let meta = span.metadata();
		match meta.target() {
			"sp_io::hashing" | "sp_io::allocator" | "sp_io::storage" => {}
			_ => log::info!("{}", meta.target()),
		}
		Id::from_u64(1)
	}

	fn record(&self, span: &Id, values: &Record<'_>) {
		// log::info!("{:?}", values);
	}

	fn record_follows_from(&self, span: &Id, follows: &Id) {
		// log::info!("{:?} follows {:?}", span, follows);
	}

	fn event(&self, event: &Event<'_>) {
		log::info!("EVENT {:?}", event);
	}

	fn enter(&self, span: &Id) {
		// log::info!("Entered Span {:?}", span);
	}

	fn exit(&self, span: &Id) {
		// log::info!("Span Exiting: {:?}", span);
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
		tracing::subscriber::set_global_default(handler).unwrap();
	}
}

/* TODO: Uncomment when wasm_tracing issues resolved
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
*/
#[async_trait::async_trait]
impl Handler<super::Die> for TracingActor {
	async fn handle(&mut self, _: super::Die, ctx: &mut Context<Self>) -> Result<()> {
		log::info!("Traces Stopping");
		ctx.stop();
		Ok(())
	}
}
