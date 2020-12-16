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
//! These traces may be used to chronologically track the execution of extrinsics inside runtime from initialization to finalization.

use super::ActorPool;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sp_runtime::traits::Block as BlockT;
use sp_tracing::{WASM_NAME_KEY, WASM_TARGET_KEY, WASM_TRACE_IDENTIFIER};
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

/// Collects traces and filters based on target.
/// The Subscriber implementation is blocking. It uses Mutex primitives to coalesce traces before
/// sending them to the appropriate actor.
/// Therefore, one must be careful not to block the async executor when adding tracing spans
/// using this subscriber implementation anywhere inside an async context in substrate-archive.
struct ArchiveTraceHandler<B: BlockT> {
	addr: Address<TracingActor<B>>,
	spans: Mutex<SpanTree>,
	targets: Vec<String>,
	counter: AtomicU64,
	current_span: CurrentSpan,
}

impl<B: BlockT> ArchiveTraceHandler<B> {
	/// Formats an event as an `EventMessage` and sends it to the TracingActor.
	fn gather_event(&self, event: &Event<'_>, time: DateTime<Utc>) -> Result<()> {
		let meta = event.metadata();
		let mut traces = TraceData::default();
		event.record(&mut traces);
		let parent_id =
			event.parent().cloned().or_else(|| self.current_span.id()).ok_or(TracingError::ParentNotFound)?;

		let (block_num, hash) = self
			.spans
			.lock()
			.0
			.get(&parent_id)
			.map(|spans| (spans.block_num(), spans.hash()))
			.ok_or(TracingError::CannotAssociateInfo(meta.target().into(), meta.name().into()))?;

		let (target, name) = if meta.name() == WASM_TRACE_IDENTIFIER {
			match (traces.0.remove(WASM_NAME_KEY), traces.0.remove(WASM_TARGET_KEY)) {
				(Some(name), Some(target)) => (name.to_string(), target.to_string()),
				(Some(name), None) => (name.to_string(), meta.name().to_string()),
				(None, Some(target)) => (meta.name().to_string(), target.to_string()),
				(None, None) => (meta.name().to_string(), meta.target().to_string()),
			}
		} else {
			(meta.name().to_string(), meta.target().to_string())
		};
		let file = traces.0.remove("file").map(Into::into);
		let line = match traces.0.remove("line").map(Into::into) {
			Some(DataType::U64(t)) => Ok(Some(t.try_into()?)),
			None => Ok(None),
			_ => Err(TracingError::TypeError),
		}?;

		let event = EventMessage {
			level: meta.level().clone(),
			target,
			name,
			parent_id,
			traces,
			block_num,
			hash,
			time,
			file,
			line,
		};

		smol::block_on(self.addr.send(event))?;
		Ok(())
	}
}

/// Stores Spans by the root Parent Id.
/// The root Parent ID is the first span that does not contain a parent id.
/// Any span proceeding the root parent will be organized by the root parent's ID, even if that span has a different parent_id.
///```
/// 				`root_parent`
/// 				 	\
/// 				 	 \
/// 				   `child0`
/// 					   \
/// 					    \
/// 					  `child1`
/// ```
/// In this case, `child1` will be in the same `CollatedSpans` category as `child0`,
/// despite `child1` having `child0` as it's `parent_id`.
struct SpanTree(HashMap<Id, CollatedSpans>);

impl SpanTree {
	fn new() -> Self {
		Self(HashMap::new())
	}

	/// Insert a span into the tree.
	///
	/// # Errors
	/// If an span has a parent_id, but the parent_id does not already have a HashMap associated with it,
	/// this function will fail.
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
				self.0.insert(span.id.clone(), CollatedSpans::new(span));
			}
		}
		Ok(())
	}

	/// Get a Span by ID.
	/// Tries to find the root_id
	/// # Returns
	/// Returns `None` if the ID does not exist in any of the span lists.
	fn get(&self, id: &Id) -> Option<&SpanMessage> {
		self.find(id).as_ref().map(|parent_id| self.0.get(parent_id).map(|list| list.get(id))).flatten().flatten()
	}

	/// Get a mutable reference to a span.
	///
	/// # Returns
	/// Returns `None` if the span cannot be found by its ID
	fn get_mut(&mut self, id: &Id) -> Option<&mut SpanMessage> {
		self.find(id)
			.as_ref()
			.map(move |parent_id| self.0.get_mut(parent_id).map(|list| list.get_mut(id)))
			.flatten()
			.flatten()
	}

	// Remove a span from the list.
	//
	// # Returns
	// Returns the span if it exists in the list, `None` if it did not exist.
	fn remove(&mut self, id: &Id) -> Option<CollatedSpans> {
		self.0.remove(id)
	}

	/// Finds an ID already in the tree, and returns the top-level (parent_id) of the given ID.
	///
	/// # Returns
	/// Returns `None` if the ID cannot be found in any of the stored span lists.
	fn find(&self, id: &Id) -> Option<Id> {
		if self.0.contains_key(id) {
			Some(id.clone())
		} else {
			self.0.iter().find(|(_, span_list)| span_list.exists(&id)).map(|(key, _)| key.clone())
		}
	}
}

/// List of spans within the same context.
#[derive(Debug)]
struct CollatedSpans(Vec<SpanMessage>);

impl Message for CollatedSpans {
	type Result = ();
}

impl CollatedSpans {
	fn new(span: SpanMessage) -> Self {
		Self(vec![span])
	}

	/// Get a span by it's ID
	///
	/// # Returns
	/// Returns `None` if the span does not exist in the list.
	fn get(&self, id: &Id) -> Option<&SpanMessage> {
		self.0.iter().find(|span| &span.id == id)
	}

	/// Get a mutable reference to a span by its ID.
	///
	/// # Returns
	/// Returns `None` if a span does not exist in the list.
	fn get_mut(&mut self, id: &Id) -> Option<&mut SpanMessage> {
		self.0.iter_mut().find(|span| &span.id == id)
	}

	/// Insert a span into the list.
	fn insert(&mut self, span: SpanMessage) {
		self.0.push(span);
	}

	/// Check if a span exists in the list.
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

/// The message a tracing subscriber collects before sending data to the TracingActor.
#[derive(Debug, Clone)]
pub struct SpanMessage {
	pub id: Id,
	pub parent_id: Option<Id>,
	pub name: String,
	pub target: String,
	pub level: Level,
	pub values: TraceData,
	pub start_time: DateTime<Utc>,
	pub overall_time: chrono::Duration,
	pub file: Option<String>,
	pub line: Option<u32>,
}

/// Stateful DataType a Tracing Value may be.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum DataType {
	Bool(bool),
	I64(i64),
	U64(u64),
	String(String),
}

impl ToString for DataType {
	fn to_string(&self) -> String {
		match self {
			DataType::Bool(b) => b.to_string(),
			DataType::I64(i) => i.to_string(),
			DataType::U64(u) => u.to_string(),
			DataType::String(s) => s.to_string(),
		}
	}
}

impl From<DataType> for String {
	fn from(data: DataType) -> String {
		data.to_string()
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
	fn new(addr: Address<TracingActor<B>>, targets: Vec<String>) -> Self {
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
			start_time: Utc::now(),
			overall_time: chrono::Duration::zero(),
			file: None,
			line: None,
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
		let time = Utc::now();
		if let Err(e) = self.gather_event(event, time) {
			log::error!("{}", e.to_string());
		}
	}

	fn enter(&self, id: &Id) {
		self.current_span.enter(id.clone());
	}

	fn exit(&self, id: &Id) {
		self.current_span.exit();
		let end_time = Utc::now();
		let mut spans = self.spans.lock();

		if let Some(span) = spans.get_mut(id) {
			span.overall_time = end_time - span.start_time;
		}

		if spans.get(id).map(|s| s.name.as_str()) == Some(BLOCK_EXEC_SPAN) {
			if let Some(spans) = spans.remove(id) {
				if let Err(_) = smol::block_on(self.addr.send(spans)) {
					log::error!("Tracing span message failed to send");
				}
			}
		}
	}
}

/// Handles collecting traces from Runtime Execution.
/// Expects to receive traces by-block. That is, each vector of Span Messages this actor receives
/// should be all the traces produces from the execution of a single block.
pub struct TracingActor<B: BlockT> {
	targets: Vec<String>,
	database: Address<ActorPool<super::DatabaseActor<B>>>,
}

impl<B: BlockT> TracingActor<B> {
	pub fn new(targets: String, database: Address<ActorPool<super::DatabaseActor<B>>>) -> Self {
		let targets = targets.split(',').map(String::from).collect();
		TracingActor { targets, database }
	}

	/// Returns true if a span is part of an enabled WASM Target.
	fn is_enabled(&self, span: &SpanMessage) -> bool {
		self.targets
			.iter()
			.filter(|t| t.as_str() != "wasm_tracing")
			.any(|t| t == &span.target || Some(t) == span.values.0.get(WASM_TARGET_KEY).map(|s| s.to_string()).as_ref())
	}

	/// Formats spans based upon data types that would be more useful for querying in the context
	/// of a relational database.
	fn format_spans(&self, spans: Vec<SpanMessage>) -> Result<Vec<SpanMessage>> {
		let format = |mut span: SpanMessage| -> Result<SpanMessage> {
			if span.name == WASM_TRACE_IDENTIFIER {
				if let Some(name) = span.values.0.remove(WASM_NAME_KEY) {
					span.name = name.to_string();
				}
				if let Some(target) = span.values.0.remove(WASM_TARGET_KEY) {
					span.target = target.to_string();
				}
				span.file = span.values.0.remove("file").map(Into::into);
				span.line = match span.values.0.remove("line") {
					Some(DataType::U64(t)) => Ok(Some(t.try_into()?)),
					None => Ok(None),
					_ => Err(TracingError::TypeError),
				}?;
			}
			Ok(span)
		};
		// if we have a different name/target from WASM replace it and remove key from TraceData
		spans.into_iter().filter(|s| s.name != BLOCK_EXEC_SPAN && self.is_enabled(&s)).map(format).collect()
	}

	/// Handles a single event message.
	async fn handle_event(&self, event: EventMessage) -> Result<()> {
		self.database.send(event.into()).await?;
		Ok(())
	}

	/// Handles CollatedSpans. Collated spans are all spans that result from the execution of a block.
	async fn handle_spans(&mut self, spans: CollatedSpans) -> Result<()> {
		let block_num = spans.block_num().ok_or(TracingError::NoBlockNumber)?;
		let hash = spans.hash().ok_or(TracingError::NoHash)?;
		let spans = self.format_spans(spans.into_inner())?;
		self.database.send(Traces::new(block_num, hash, spans).into()).await?;
		Ok(())
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Actor for TracingActor<B> {
	async fn started(&mut self, ctx: &mut Context<Self>) {
		let addr = ctx.address().expect("Actor just started");
		let handler = ArchiveTraceHandler::new(addr.clone(), self.targets.clone());
		log::debug!("Trace Targets [{}]", self.targets.join(",").as_str());
		if let Err(_) = tracing::subscriber::set_global_default(handler) {
			log::warn!("Global default subscriber already set elsewhere");
		}
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<CollatedSpans> for TracingActor<B> {
	async fn handle(&mut self, msg: CollatedSpans, ctx: &mut Context<Self>) {
		match self.handle_spans(msg).await {
			Err(Disconnected) => ctx.stop(),
			Err(e) => log::error!("{}", e.to_string()),
			Ok(()) => (),
		}
	}
}

/// Finished Trace Data Format. Ready for insertion into a relational database.
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

	/// Get the hash these spans come from.
	pub fn hash(&self) -> Vec<u8> {
		self.hash.clone()
	}

	/// Get the block number of the block these spans come from.
	pub fn block_num(&self) -> u32 {
		self.block_num
	}
}

/// The Event a tracing subscriber collects before sending data to the TracingActor.
#[derive(Debug)]
pub struct EventMessage {
	pub block_num: Option<u32>,
	pub hash: Option<Vec<u8>>,
	pub name: String,
	pub target: String,
	pub level: Level,
	pub traces: TraceData,
	pub parent_id: Id,
	pub time: DateTime<Utc>,
	pub file: Option<String>,
	pub line: Option<u32>,
}

impl Message for EventMessage {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<EventMessage> for TracingActor<B> {
	async fn handle(&mut self, event: EventMessage, ctx: &mut Context<Self>) {
		match self.handle_event(event).await {
			Err(Disconnected) => ctx.stop(),
			Err(e) => log::error!("{}", e.to_string()),
			Ok(()) => (),
		}
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
