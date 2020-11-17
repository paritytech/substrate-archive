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
	tracing_targets: String,
}

impl ArchiveTraceHandler {
	fn new(addr: Address<TracingActor>, tracing_targets: String) -> Self {
		Self { addr, tracing_targets }
	}
}

impl TraceHandler for ArchiveTraceHandler {
	fn handle_span(&self, sd: SpanDatum) {
		println!("S");
		self.addr.do_send(SpanMessage(sd)).unwrap();
	}

	fn handle_event(&self, ev: TraceEvent) {
		println!("E");
		self.addr.do_send(EventMessage(ev)).unwrap();
	}
}

pub struct TracingActor {
	layer: Option<ProfilingLayer>,
	targets: String,
}

impl TracingActor {
	pub fn new(targets: String) -> Self {
		TracingActor { layer: None, targets }
	}
}

#[async_trait::async_trait]
impl Actor for TracingActor {
	async fn started(&mut self, ctx: &mut Context<Self>) {
		log::info!("Starting State Tracing");
		let addr = ctx.address().expect("Actor just started");
		let handler = ArchiveTraceHandler::new(addr.clone(), self.targets.clone());
		let layer = ProfilingLayer::new_with_handler(Box::new(handler), self.targets.as_str());
		let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish().with(layer);
		// self.layer = Some(layer);
		tracing::subscriber::set_global_default(subscriber).unwrap();
	}

	async fn stopped(&mut self, _: &mut Context<Self>) {
		self.layer = None;
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
		log::info!("Span: {:?}", msg);
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
		log::info!("Traces dying");
		ctx.stop();
		Ok(())
	}
}
