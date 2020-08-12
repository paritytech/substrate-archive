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

use sc_tracing::{SpanDatum, TraceEvent, TraceHandler, ProfilingSubscriber};
use xtra::prelude::*;

const TRACE_TARGETS: &str = "assets,atomic-swap,aura,authority-discovery,authorship,babe,balances,collective,contracts,democracy,elections,elections-phragmen,evm,executive,finality-tracker,generic-asset,grandpa,identity,im-online,indices,membership,metadata,multisig,nicks,offences,proxy,randomness-collective-flip,recovery,scheduler,scored-pool,session,society,staking,sudo,support,system,timestamp,transaction-payment,treasury,utility,vesting";

#[derive(Clone)]
pub struct ArchiveTraceHandler {
    addr: Option<Address<Self>>
}

impl ArchiveTraceHandler {
    pub fn new() -> Self {
        sp_tracing::wasm_tracing_enabled();
        Self {
            addr: None
        }
    }
}

impl TraceHandler for ArchiveTraceHandler {
    fn handle_span(&self, sd: SpanDatum) {
        println!("Outside");
        if let Some(a) = self.addr.as_ref() {
            println!("Inside");
            let _ = a.do_send(SpanMessage(sd));
        }
    }

    fn handle_event(&self, ev: TraceEvent) {
        println!("Outside");
        if let Some(a) = self.addr.as_ref() {
            println!("Inside");
            let _ = a.do_send(EventMessage(ev));
        }
    }
}

impl Actor for ArchiveTraceHandler {
    fn started(&mut self, ctx: &mut Context<Self>) {
        let sub = ProfilingSubscriber::new_with_handler(Box::new(self.clone()), TRACE_TARGETS);
        let addr = ctx.address().expect("Actor just started");
        self.addr = Some(addr);
        tracing::subscriber::set_global_default(sub).unwrap();
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        self.addr = None;
    }
}

#[derive(Debug)]
struct SpanMessage(SpanDatum);


impl Message for SpanMessage {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<SpanMessage> for ArchiveTraceHandler {
    async fn handle(&mut self, msg: SpanMessage, ctx: &mut Context<Self>) {
        println!("Span: {:?}", msg);
    }
}

#[derive(Debug)]
struct EventMessage(TraceEvent);


impl Message for EventMessage {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<EventMessage> for ArchiveTraceHandler {
    async fn handle(&mut self, msg: EventMessage, ctx: &mut Context<Self>) {
        println!("Event: {:?}", msg);
    }
}

