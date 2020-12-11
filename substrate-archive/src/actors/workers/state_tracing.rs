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

//! The State Tracing Actor
//!
//! Running this Actor collects traces from running WASM that has been compiled with the `with-tracing` feature.
//! These traces may be used to track the execution of extrinsics and the runtime from initialization to finalization.
//!
//! # Warn
//! The way the tracing actor exists today is fundamentally blocking. It uses Mutex primitives to coalesce traces before
//! sending them to the appropriate actor.
//! Therefore, one must be careful not to block the async executor when adding tracing spans (if ever required) elsewhere in substrate-archive.
//!
//!

use super::ActorPool;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sp_runtime::traits::Block as BlockT;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicU64, Ordering};
use substrate_archive_common::{Error::Disconnected, Result, TracingError};
use tracing::{
	event::Event,
	field::{Field, Visit},
	span::{Attributes, Id, Record},
	Level, Metadata, Subscriber,
};
use tracing_subscriber::CurrentSpan;
use xtra::prelude::*;

pub const BLOCK_EXEC_SPAN: &str = "block_execute_task";

struct ArchiveTraceHandler<B: BlockT> {
	addr: Address<TracingActor<B>>,
	spans: Mutex<SpanTree>,
	targets: Vec<String>,
	counter: AtomicU64,
	current_span: CurrentSpan,
}

struct SpanTree(HashMap<Id, SortedSpans>);

impl SpanTree {
	fn new() -> Self {
		Self(HashMap::new())
	}

	fn insert(&mut self, span: SpanMessage) -> Result<()> {
		match &span.parent_id {
			Some(id) => match self.find(&id) {
				Some(top_id) => {
					self.0.get_mut(&top_id).expect("Must be ID since it was found").insert(span);
				}
				None => {
					return Err(TracingError::MissingTree.into());
				}
			},
			None => {
				self.0.insert(span.id.clone(), SortedSpans::new(span));
			}
		}
		Ok(())
	}

	fn get(&self, id: &Id) -> Option<&SpanMessage> {
		self.find(id).as_ref().map(|parent_id| self.0.get(parent_id).map(|list| list.get(id))).flatten().flatten()
	}

	fn get_mut(&mut self, id: &Id) -> Option<&mut SpanMessage> {
		self.find(id)
			.as_ref()
			.map(move |parent_id| self.0.get_mut(parent_id).map(|list| list.get_mut(id)))
			.flatten()
			.flatten()
	}

	fn remove(&mut self, id: &Id) -> Option<SortedSpans> {
		self.0.remove(id)
	}

	/// Finds an ID already in the tree, and returns the top-level (parent_id) of the given ID.
	fn find(&self, id: &Id) -> Option<Id> {
		if self.0.contains_key(id) {
			Some(id.clone())
		} else {
			self.0.iter().find(|(_, span_list)| span_list.exists(&id)).map(|(key, _)| key.clone())
		}
	}
}

/// List of Spans sorted by ID
#[derive(Debug)]
struct SortedSpans(Vec<SpanMessage>);

impl Message for SortedSpans {
	type Result = ();
}

impl SortedSpans {
	fn new(span: SpanMessage) -> Self {
		Self(vec![span])
	}

	fn get(&self, id: &Id) -> Option<&SpanMessage> {
		self.0.iter().find(|span| &span.id == id)
	}

	fn get_mut(&mut self, id: &Id) -> Option<&mut SpanMessage> {
		self.0.iter_mut().find(|span| &span.id == id)
	}

	fn insert(&mut self, span: SpanMessage) {
		let position = self.0.binary_search_by_key(&span.id.into_u64(), |s| s.id.into_u64()).unwrap_or_else(|id| id);
		self.0.insert(position, span);
	}

	fn exists(&self, id: &Id) -> bool {
		self.0.iter().any(|other_id| &other_id.id == id)
	}

	/// Tries to get the block number from a set of tracing data.
	/// Returns `None` if the block number cannot be found.
	pub fn block_num(&self) -> Option<u32> {
		let root_span = self.0.iter().find(|s| s.name == BLOCK_EXEC_SPAN)?;
		match root_span.values.0.get("number") {
			Some(DataType::U64(num)) => Some((*num).try_into().ok()).flatten(),
			Some(DataType::String(s)) => s.parse().ok(),
			_ => None,
		}
	}

	/// Tries to get the hash of the executed block from the tracing data.
	/// Returns `None` if it cannot be found.
	pub fn hash(&self) -> Option<Vec<u8>> {
		let root_span = self.0.iter().find(|s| s.name == BLOCK_EXEC_SPAN)?;
		match root_span.values.0.get("hash") {
			Some(DataType::String(s)) => hex::decode(s).ok(),
			_ => None,
		}
	}

	fn into_inner(self) -> Vec<SpanMessage> {
		self.0
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
		let spans = Mutex::new(SpanTree::new());
		Self { addr, targets, counter, spans, current_span: Default::default() }
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

		if let Err(e) = self.spans.lock().insert(span_message) {
			log::error!("{}", e);
		}

		id
	}

	fn record(&self, id: &Id, values: &Record<'_>) {
		if let Some(span) = self.spans.lock().get_mut(id) {
			values.record(&mut span.values);
		}
	}

	fn record_follows_from(&self, _: &Id, _: &Id) {
		log::warn!("Followed span relationship not recorded");
	}

	fn event(&self, event: &Event<'_>) {
		let meta = event.metadata();
		let mut values = Default::default();
		event.record(&mut values);
		let parent_id = event.parent().cloned().or_else(|| self.current_span.id());
		let block_info = match &parent_id {
			Some(id) => self.spans.lock().0.get(id).map(|spans| (spans.block_num(), spans.hash())),
			None => {
				// this happens when an event is declared outside of the context of a Span.
				// WASM tracing should always be within the context of a span, at the very least BLOCK_EXEC_SPAN.
				log::warn!(
					"Discarding event {}:{}; Cannot be associated with a block number and/or hash!",
					meta.target(),
					meta.name()
				);
				return;
			}
		};

		let event = if let Some((block_num, hash)) = block_info {
			EventMessage {
				name: meta.name().to_string(),
				target: meta.target().to_string(),
				level: meta.level().clone(),
				parent_id,
				values,
				block_num,
				hash,
			}
		} else {
			return;
		};

		if let Err(_) = smol::block_on(self.addr.send(event)) {
			log::error!("Event message failed to send")
		}
	}

	fn enter(&self, id: &Id) {
		self.current_span.enter(id.clone());
	}

	fn exit(&self, id: &Id) {
		let mut spans = self.spans.lock();
		if spans.get(id).map(|s| s.name.as_str()) == Some(BLOCK_EXEC_SPAN) {
			if let Some(spans) = spans.remove(id) {
				if let Err(_) = smol::block_on(self.addr.send(spans)) {
					log::error!("Tracing span message failed to send");
				}
			}
		}
		self.current_span.exit();
	}
}

pub struct TracingActor<B: BlockT> {
	targets: String,
	database: Address<ActorPool<super::DatabaseActor<B>>>,
}

impl<B: BlockT> TracingActor<B> {
	pub fn new(targets: String, database: Address<ActorPool<super::DatabaseActor<B>>>) -> Self {
		TracingActor { targets, database }
	}

	fn format_spans(&self, spans: Vec<SpanMessage>) -> Vec<SpanMessage> {
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
		spans.into_iter().filter(|s| s.name != BLOCK_EXEC_SPAN).map(format).collect()
	}

	async fn handle_event(&self, event: EventMessage) -> Result<()> {}

	async fn handle_spans(&mut self, spans: SortedSpans) -> Result<()> {
		let block_num = spans.block_num().ok_or(TracingError::NoBlockNumber)?;
		let hash = spans.hash().ok_or(TracingError::NoHash)?;
		let spans = self.format_spans(spans.into_inner());
		self.database.send(Traces::new(block_num, hash, spans).into()).await?;
		Ok(())
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Actor for TracingActor<B> {
	async fn started(&mut self, ctx: &mut Context<Self>) {
		let addr = ctx.address().expect("Actor just started");
		let handler = ArchiveTraceHandler::new(addr.clone(), self.targets.clone());
		log::debug!("Trace Targets [{}]", self.targets.as_str());
		if let Err(_) = tracing::subscriber::set_global_default(handler) {
			log::warn!("Global default subscriber already set elsewhere");
		}
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<SortedSpans> for TracingActor<B> {
	async fn handle(&mut self, msg: SortedSpans, ctx: &mut Context<Self>) {
		match self.handle_spans(msg).await {
			Err(Disconnected) => ctx.stop(),
			Err(e) => log::error!("{}", e.to_string()),
			Ok(()) => (),
		}
	}
}

#[derive(Debug)]
pub struct Traces {
	block_num: u32,
	hash: Vec<u8>,
	pub spans: Vec<SpanMessage>,
}

impl Message for Traces {
	type Result = ();
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

#[derive(Debug)]
pub struct EventMessage {
	block_num: Option<u32>,
	hash: Option<Vec<u8>>,
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
		self.database.send(msg.into()).await.unwrap();
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
