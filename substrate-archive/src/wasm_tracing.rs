// Copyright 2017-2021 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

//! The Layer implementation for Tracing
/// Tracing allows for collecting more detailed information
/// about the execution of blocks, associated values for extrinsics being executed,
/// as well as more information about how storage was collected.
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sp_tracing::{WASM_NAME_KEY, WASM_TARGET_KEY, WASM_TRACE_IDENTIFIER};
use tracing::{
	dispatcher,
	event::Event,
	field::{Field, Visit},
	span::{Attributes, Id, Record},
	Dispatch, Level, Metadata,
};
use tracing_subscriber::{
	layer::{Context, SubscriberExt},
	Layer, Registry,
};

use crate::error::{Result, TracingError};

/// The Event a tracing subscriber collects before sending data to the TracingActor.
#[derive(Debug)]
pub struct EventMessage {
	pub name: String,
	pub target: String,
	pub level: Level,
	pub values: TraceData,
	pub parent_id: Option<Id>,
	pub time: DateTime<Utc>,
	pub file: Option<String>,
	pub line: Option<u32>,
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

/// Finished Trace Data Format. Ready for insertion into a relational database.
#[derive(Debug, Default)]
pub struct Traces {
	block_num: u32,
	hash: Vec<u8>,
	pub spans: Vec<SpanMessage>,
	pub events: Vec<EventMessage>,
}

impl Traces {
	pub fn new(block_num: u32, hash: Vec<u8>, events: Vec<EventMessage>, spans: Vec<SpanMessage>) -> Self {
		Traces { block_num, hash, spans, events }
	}

	/// Get the hash these spans come from.
	pub fn hash(&self) -> &[u8] {
		&self.hash
	}

	/// Get the block number of the block these spans come from.
	pub fn block_num(&self) -> u32 {
		self.block_num
	}
}

#[derive(Debug)]
pub struct SpansAndEvents {
	pub spans: Vec<SpanMessage>,
	pub events: Vec<EventMessage>,
}

/// Collects traces and filters based on target.
/// The Layer implementation is blocking. It uses Mutex primitives to coalesce traces before
/// sending them to the appropriate actor.
/// Therefore, one must be careful not to block the async executor when adding tracing spans
/// using this subscriber implementation anywhere inside an async context in substrate-archive.
pub struct TraceHandler {
	span_events: Arc<Mutex<SpansAndEvents>>,
	targets: Vec<(String, Level)>,
}

impl TraceHandler {
	pub fn new(targets: &str, span_events: Arc<Mutex<SpansAndEvents>>) -> Self {
		let mut targets: Vec<_> = targets.split(',').map(|s| parse_target(s)).collect();
		targets.push((WASM_TRACE_IDENTIFIER.to_string(), Level::TRACE));
		Self { span_events, targets }
	}

	/// Formats an event as an [`EventMessage`] and stores it in the [`SpansAndEvents`]
	/// (which is sent to the [`StorageAggregator`] after the block is executed).
	fn gather_event(&self, event: &Event<'_>, time: DateTime<Utc>, ctx: &Context<'_, Registry>) -> Result<()> {
		let meta = event.metadata();
		let mut values = TraceData::default();
		event.record(&mut values);
		let parent_id = event.parent().cloned().or_else(|| ctx.lookup_current().map(|c| c.id()));

		// check if WASM traces specify a different name/target.
		let name = values.0.remove(WASM_NAME_KEY).map(|t| t.to_string()).unwrap_or_else(|| meta.name().to_string());
		let target =
			values.0.remove(WASM_TARGET_KEY).map(|t| t.to_string()).unwrap_or_else(|| meta.target().to_string());

		let file = values.0.remove("file").map(Into::into);
		let line = match values.0.remove("line").map(Into::into) {
			Some(DataType::U64(t)) => Ok(Some(t.try_into()?)),
			None => Ok(None),
			_ => Err(TracingError::TypeError),
		}?;

		let event = EventMessage { level: *meta.level(), target, name, parent_id, values, time, file, line };
		self.span_events.lock().events.push(event);
		Ok(())
	}

	// we need this because we don't know the values until after tracing has been executed
	/// Returns true if a span is part of an enabled Target. Checks WASM in addition to the spans target.
	#[allow(clippy::suspicious_operation_groupings)]
	fn is_enabled(&self, span: &SpanMessage) -> bool {
		let wasm_target = span.values.0.get(WASM_TARGET_KEY).map(|s| s.to_string());
		self.targets.iter().filter(|t| t.0.as_str() != "wasm_tracing").any(|t| {
			let wanted_target = &t.0.as_str();
			let valid_native_target = span.target.starts_with(wanted_target);
			let valid_wasm_target = wasm_target.as_ref().map(|wt| wt.starts_with(wanted_target)).unwrap_or(false);
			(valid_native_target || valid_wasm_target) && span.level <= t.1
		})
	}

	/// Formats spans based upon data types that are more useful for querying in the context
	/// of a relational database.
	fn gather_span(&self, mut span: SpanMessage) -> Result<()> {
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

		self.span_events.lock().spans.push(span);
		Ok(())
	}

	/// Start tracing with the predicate `fun`.
	/// Consumes this TraceHandler.
	pub fn scoped_trace<T>(self, fun: impl FnOnce() -> Result<T>) -> Result<(Vec<SpanMessage>, Vec<EventMessage>, T)> {
		let span_events = self.span_events.clone();
		let subscriber = Registry::default().with(self);
		let dispatch = Dispatch::new(subscriber);
		let res = dispatcher::with_default(&dispatch, fun)?;

		let mut traces = span_events.lock();
		let spans = traces.spans.drain(..).collect::<Vec<SpanMessage>>();
		let events = traces.events.drain(..).collect::<Vec<EventMessage>>();

		Ok((spans, events, res))
	}
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

impl Layer<Registry> for TraceHandler {
	fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, Registry>) -> bool {
		self.targets.iter().any(|(t, _l)| metadata.target().starts_with(t.as_str()))
	}

	fn new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, Registry>) {
		let meta = attrs.metadata();
		let mut values = TraceData::default();
		attrs.record(&mut values);

		let span_message = SpanMessage {
			id: id.clone(),
			parent_id: attrs.parent().cloned().or_else(|| ctx.lookup_current().map(|c| c.id())),
			name: meta.name().to_string(),
			target: meta.target().to_string(),
			level: *meta.level(),
			start_time: Utc::now(),
			overall_time: chrono::Duration::zero(),
			file: None,
			line: None,
			values,
		};
		if self.is_enabled(&span_message) {
			self.gather_span(span_message).unwrap_or_else(|e| log::error!("{}", e.to_string()));
		}
	}

	fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, Registry>) {
		if let Some(span) = self.span_events.lock().spans.iter_mut().find(|span| &span.id == id) {
			values.record(&mut span.values);
		}
	}

	fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
		let time = Utc::now();
		if let Err(e) = self.gather_event(event, time, &ctx) {
			log::error!("{}", e.to_string());
		}
	}

	fn on_close(&self, id: Id, _ctx: Context<'_, Registry>) {
		let end_time = Utc::now();
		if let Some(span) = self.span_events.lock().spans.iter_mut().find(|span| span.id == id) {
			span.overall_time = end_time - span.start_time;
		}
	}
}

// Default to TRACE if no level given or unable to parse Level
// We do not support a global `Level` currently
fn parse_target(s: &str) -> (String, Level) {
	match s.find('=') {
		Some(i) => {
			let target = s[0..i].to_string();
			if s.len() > i {
				let level = s[i + 1..s.len()].parse::<Level>().unwrap_or(Level::TRACE);
				(target, level)
			} else {
				(target, Level::TRACE)
			}
		}
		None => (s.to_string(), Level::TRACE),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use anyhow::Error;
	use sc_executor::{WasmExecutionMethod, WasmExecutor};
	use sc_executor_common::runtime_blob::RuntimeBlob;
	use sp_io::TestExternalities;
	use sp_wasm_interface::HostFunctions;
	use test_common::wasm_binary_unwrap;

	const TARGETS: &str = "wasm_tracing,test_wasm";

	#[test]
	fn should_collect_spans_and_events_in_wasm() -> Result<(), Error> {
		let mut ext = TestExternalities::default();
		let mut ext = ext.ext();

		let executor = WasmExecutor::new(
			WasmExecutionMethod::Compiled,
			Some(8),
			sp_io::SubstrateHostFunctions::host_functions(),
			8,
			None,
		);

		let span_events = Arc::new(Mutex::new(SpansAndEvents { spans: Vec::new(), events: Vec::new() }));
		let handler = TraceHandler::new(&TARGETS, span_events.clone());
		let (spans, events, _) = handler.scoped_trace(|| {
			executor
				.uncached_call(
					RuntimeBlob::uncompress_if_needed(&wasm_binary_unwrap()[..]).unwrap(),
					&mut ext,
					true,
					"test_trace_handler",
					&[],
				)
				.unwrap();
			Ok(())
		})?;

		assert_eq!(spans[0].name, "im_a_span");
		assert_eq!(spans[0].target, "test_wasm");
		assert_eq!(spans[0].target, "test_wasm");
		assert_eq!(spans[0].id, Id::from_u64(2));
		assert_eq!(spans[1].id, Id::from_u64(3));
		assert_eq!(spans[1].parent_id, Some(Id::from_u64(2)));
		assert_eq!(events[0].target, "test_wasm");
		Ok(())
	}
}
