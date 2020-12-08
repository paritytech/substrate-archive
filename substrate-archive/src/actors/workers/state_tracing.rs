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

use super::ActorPool;
use serde::{Deserialize, Serialize};
use sp_runtime::traits::Block as BlockT;
use std::collections::HashMap;
use std::convert::TryInto;
use std::iter::FromIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use substrate_archive_common::{Result, TracingError};
use tracing::{
	event::Event,
	field::{Field, Visit},
	span::{Attributes, Id, Record},
	Level, Metadata, Subscriber,
};
use tracing_subscriber::CurrentSpan;
use xtra::prelude::*;

pub const BLOCK_START_EXEC: &str = "block_execute_task";
pub const BLOCK_END_EXEC: &str = "block_end_execute";

struct ArchiveTraceHandler<B: BlockT> {
	addr: Address<TracingActor<B>>,
	targets: Vec<String>,
	counter: AtomicU64,
	current_span: CurrentSpan,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Tracing<B: BlockT> {
	block_num: u32,
	hash: B::Hash,
	target: String,
	name: String,
	values: TraceData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum DataType {
	Bool(bool),
	I64(i64),
	U64(u64),
	String(String),
}

impl From<DataType> for String {
	fn from(data: DataType) -> String {
		match data {
			DataType::Bool(b) => format!("{}", b),
			DataType::I64(i) => format!("{}", i),
			DataType::U64(u) => format!("{}", u),
			DataType::String(s) => s,
		}
	}
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct TraceData(HashMap<String, DataType>);

impl Visit for TraceData {
	fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
		self.0.insert(field.name().to_string(), DataType::String(format!("{:?}", value)));
	}

	fn record_i64(&mut self, field: &Field, value: i64) {
		self.0.insert(field.name().to_string(), DataType::I64(value));
	}

	fn record_u64(&mut self, field: &Field, value: u64) {
		self.0.insert(field.name().to_string(), DataType::U64(value));
	}

	fn record_str(&mut self, field: &Field, value: &str) {
		self.0.insert(field.name().to_string(), DataType::String(value.to_string()));
	}

	fn record_bool(&mut self, field: &Field, value: bool) {
		self.0.insert(field.name().to_string(), DataType::Bool(value));
	}
}

impl<B: BlockT> ArchiveTraceHandler<B> {
	fn new(addr: Address<TracingActor<B>>, targets: String) -> Self {
		let targets = targets.split(',').map(String::from).collect();
		// must start indexing from 1 otherwise `tracing` panics
		let counter = AtomicU64::new(1);
		Self { addr, targets, counter, current_span: Default::default() }
	}
}

impl<B: BlockT> Subscriber for ArchiveTraceHandler<B> {
	fn enabled(&self, metadata: &Metadata<'_>) -> bool {
		self.targets.iter().any(|t| t == metadata.target()) || metadata.target() == "substrate_archive::tasks"
	}

	fn new_span(&self, attrs: &Attributes<'_>) -> Id {
		let meta = attrs.metadata();
		let mut values = TraceData::default();
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
		if let Err(_) = smol::block_on(self.addr.send(span_message)) {
			log::error!("Tracing span message failed to send");
		}
		id
	}

	fn record(&self, span: &Id, values: &Record<'_>) {
		unimplemented!();
	}

	fn record_follows_from(&self, span: &Id, follows: &Id) {
		unimplemented!();
	}

	fn event(&self, event: &Event<'_>) {
		let meta = event.metadata();
		let mut values = Default::default();
		event.record(&mut values);
		let event = EventMessage {
			name: meta.name().to_string(),
			target: meta.target().to_string(),
			level: meta.level().clone(),
			parent_id: event.parent().cloned().or_else(|| self.current_span.id()),
			values,
		};
		if let Err(_) = smol::block_on(self.addr.send(event)) {
			log::error!("Event message failed to send")
		}
	}

	fn enter(&self, id: &Id) {
		self.current_span.enter(id.clone());
	}

	fn exit(&self, span: &Id) {
		self.current_span.exit();
	}
}

pub struct TracingActor<B: BlockT> {
	targets: String,
	span_tree: HashMap<Id, HashMap<Id, SpanMessage>>,
	database: Address<ActorPool<super::DatabaseActor<B>>>,
}

impl<B: BlockT> TracingActor<B> {
	pub fn new(targets: String, database: Address<ActorPool<super::DatabaseActor<B>>>) -> Self {
		TracingActor { targets, database, span_tree: HashMap::new() }
	}

	//// Find the key in `span_tree` that contains a map which has `id`.
	fn find_root<'a>(&'a self, id: &'a Id) -> Option<&'a Id> {
		if self.span_tree.contains_key(id) {
			Some(id)
		} else {
			self.span_tree.iter().find(|(_, map)| map.contains_key(id)).map(|m| m.0)
		}
	}

	fn format_messages(&self, msgs: Vec<SpanMessage>) -> Vec<SpanMessage> {
		// if we have a different name/target from WASM replace it and remove key from TraceData
		let format = |mut span: SpanMessage| match (span.values.0.remove("name"), span.values.0.remove("target")) {
			(Some(name), Some(target)) => SpanMessage {
				id: span.id,
				parent_id: span.parent_id,
				level: span.level,
				values: span.values,
				name: name.into(),
				target: target.into(),
			},
			(Some(name), None) => SpanMessage {
				id: span.id,
				parent_id: span.parent_id,
				level: span.level,
				values: span.values,
				target: span.target,
				name: name.into(),
			},
			(None, Some(target)) => SpanMessage {
				id: span.id,
				parent_id: span.parent_id,
				level: span.level,
				values: span.values,
				name: span.name,
				target: target.into(),
			},
			(None, None) => span,
		};
		msgs.into_iter().filter(|s| s.name != BLOCK_START_EXEC && s.name != BLOCK_END_EXEC).map(format).collect()
	}

	/// Tries to get the block number from a set of tracing data.
	/// Returns `None` if the block number cannot be found.
	pub fn block_num_from_spans(&self, spans: &[SpanMessage]) -> Option<u32> {
		let span = spans.iter().find(|s| s.name == BLOCK_START_EXEC)?;
		match span.values.0.get("number") {
			Some(DataType::U64(num)) => Some((*num).try_into().ok()).flatten(),
			Some(DataType::String(s)) => s.parse().ok(),
			_ => None,
		}
	}

	/// Tries to get the hash of the executed block from the tracing data.
	/// Returns `None` if it cannot be found.
	pub fn hash_from_spans(&self, spans: &[SpanMessage]) -> Option<Vec<u8>> {
		let span = spans.iter().find(|s| s.name == BLOCK_START_EXEC)?;
		match span.values.0.get("hash") {
			Some(DataType::String(s)) => hex::decode(s).ok(),
			_ => None,
		}
	}

	async fn handle_span(&mut self, msg: SpanMessage, ctx: &mut Context<Self>) -> Result<()> {
		if msg.name == BLOCK_END_EXEC {
			let tracing_messages = self
				.span_tree
				.remove(&msg.parent_id.ok_or(TracingError::ParentNotFound)?)
				.ok_or(TracingError::UnknownStartSpan)?;

			// TODO: If/when `into_values()` stabilizes, prefer that here
			// https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.into_values
			let mut spans: Vec<SpanMessage> = Vec::from_iter(tracing_messages.values().cloned());
			spans.sort_by(|a, b| a.id.into_u64().cmp(&b.id.into_u64()));

			let block_num = self.block_num_from_spans(&spans).ok_or(TracingError::NoBlockNumber)?;
			let hash = self.hash_from_spans(&spans).ok_or(TracingError::NoHash)?;
			let spans = self.format_messages(spans);

			// lifetime for `ctx` in the context of awaiting a future
			// means we need to separate the let binding from the `if let Some`
			let addr = ctx.address();
			if let Some(addr) = addr {
				ctx.handle_while(self, addr.send(Traces::new(block_num, hash, spans))).await?;
			}
		} else {
			match &msg.parent_id {
				Some(id) => {
					let tree_id = self.find_root(&id).ok_or(TracingError::ParentNotFound)?.clone();
					let nested = self.span_tree.get_mut(&tree_id).ok_or(TracingError::MissingTree)?;
					nested.insert(msg.id.clone(), msg);
				}
				None => {
					let mut new_map = HashMap::new();
					let root_id = msg.id.clone();
					new_map.insert(root_id.clone(), msg);
					self.span_tree.insert(root_id, new_map);
				}
			}
		}
		Ok(())
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Actor for TracingActor<B> {
	async fn started(&mut self, ctx: &mut Context<Self>) {
		println!("State Tracing Started");
		let addr = ctx.address().expect("Actor just started");
		let handler = ArchiveTraceHandler::new(addr.clone(), self.targets.clone());
		log::debug!("Trace Targets [{}]", self.targets.as_str());
		if let Err(_) = tracing::subscriber::set_global_default(handler) {
			log::warn!("Global default subscriber already set elsewhere");
		}
	}
}

#[derive(Debug, Clone)]
pub struct SpanMessage {
	pub id: Id,
	pub parent_id: Option<Id>,
	pub name: String,
	pub target: String,
	pub level: Level,
	pub values: TraceData,
}

impl Message for SpanMessage {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<SpanMessage> for TracingActor<B> {
	async fn handle(&mut self, msg: SpanMessage, ctx: &mut Context<Self>) {
		if let Err(e) = self.handle_span(msg, ctx).await {
			log::error!("{}", e.to_string());
		}
	}
}

#[derive(Debug)]
pub struct Traces {
	block_num: u32,
	hash: Vec<u8>,
	pub spans: Vec<SpanMessage>,
}

impl Traces {
	pub fn new(block_num: u32, hash: Vec<u8>, spans: Vec<SpanMessage>) -> Self {
		Traces { block_num, hash, spans }
	}

	pub fn hash(&self) -> Vec<u8> {
		self.hash.clone()
	}

	pub fn block_num(&self) -> u32 {
		self.block_num
	}
}

impl Message for Traces {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<Traces> for TracingActor<B> {
	async fn handle(&mut self, msg: Traces, ctx: &mut Context<Self>) {
		if let Err(_) = self.database.send(msg.into()).await {
			ctx.stop();
		}
	}
}

#[derive(Debug)]
struct EventMessage {
	name: String,
	target: String,
	level: Level,
	values: TraceData,
	parent_id: Option<Id>,
}

impl Message for EventMessage {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<EventMessage> for TracingActor<B> {
	async fn handle(&mut self, msg: EventMessage, _: &mut Context<Self>) {
		log::info!("Event: {:?}", msg);
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<super::Die> for TracingActor<B> {
	async fn handle(&mut self, _: super::Die, ctx: &mut Context<Self>) -> Result<()> {
		log::info!("Traces Stopping");
		ctx.stop();
		Ok(())
	}
}
