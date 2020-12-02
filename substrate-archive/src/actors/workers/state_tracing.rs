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

use sc_tracing::Values;
use std::sync::atomic::{AtomicU64, Ordering};
use substrate_archive_common::Result;
use tracing::{
	event::Event,
	span::{Attributes, Id, Record},
	Level, Metadata, Subscriber,
};
use tracing_subscriber::CurrentSpan;

// use tracing_subscriber::{layer::SubscriberExt, EnvFilter};
use xtra::prelude::*;

struct ArchiveTraceHandler {
	addr: Address<TracingActor>,
	targets: Vec<String>,
	counter: AtomicU64,
	current_span: CurrentSpan,
}

impl ArchiveTraceHandler {
	fn new(addr: Address<TracingActor>, targets: String) -> Self {
		let targets = targets.split(',').map(String::from).collect();
		// must start indexing from 1 otherwise `tracing` panics
		let counter = AtomicU64::new(1);
		Self { addr, targets, counter, current_span: Default::default() }
	}
}

impl Subscriber for ArchiveTraceHandler {
	fn enabled(&self, metadata: &Metadata<'_>) -> bool {
		println!("{}", metadata.target());
		self.targets.iter().any(|t| t == metadata.target()) || metadata.target() == "substrate_archive::tasks"
	}

	fn new_span(&self, attrs: &Attributes<'_>) -> Id {
		let meta = attrs.metadata();
		let mut values = Values::default();
		attrs.record(&mut values);
		let id = Id::from_u64(self.counter.fetch_add(1, Ordering::Relaxed));
		let span_message = SpanMessage {
			id: id.clone(),
			parent_id: attrs.parent().cloned().or_else(|| self.current_span.id()),
			name: meta.name().to_string(),
			target: meta.target().to_string(),
			level: meta.level().clone(),
			values,
		};
		smol::block_on(self.addr.send(span_message)).unwrap();
		id
	}

	fn record(&self, span: &Id, values: &Record<'_>) {
		// log::info!("{:?}", values);
	}

	fn record_follows_from(&self, span: &Id, follows: &Id) {
		// log::info!("{:?} follows {:?}", span, follows);
	}

	fn event(&self, event: &Event<'_>) {
		println!("EVENT {:?}", event);
	}

	fn enter(&self, id: &Id) {
		self.current_span.enter(id.clone());
	}

	fn exit(&self, span: &Id) {
		self.current_span.exit();
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
		println!("State Tracing Started");
		let addr = ctx.address().expect("Actor just started");
		let handler = ArchiveTraceHandler::new(addr.clone(), self.targets.clone());
		log::debug!("Trace Targets [{}]", self.targets.as_str());
		tracing::subscriber::set_global_default(handler).unwrap();
	}
}

#[derive(Debug)]
struct SpanMessage {
	pub id: Id,
	pub parent_id: Option<Id>,
	pub name: String,
	pub target: String,
	pub level: Level,
	pub values: Values,
}

impl Message for SpanMessage {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<SpanMessage> for TracingActor {
	async fn handle(&mut self, msg: SpanMessage, _: &mut Context<Self>) {
		println!("{:?}", msg);
	}
}

#[derive(Debug)]
struct EventMessage;

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
