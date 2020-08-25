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

use std::error::Error;
use xtra::prelude::*;
use xtra::{Disconnected, MessageResponseFuture};

/// A system in which actors run
struct System {
    name: String,
}

impl System {
    pub fn new<T: Into<String>>(name: T) -> Self {

        Self {
            name: name.into()
        }
    }

    pub fn register_error_handler(handler: impl Fn(&dyn Error)) {
        unimplemented!()
    }

    /// Stop the system
    pub fn stop(&self) {
        unimplemented!()
    }
}

/// Trait that marks this struct as it's failures being able to be handled by a Supervisor
pub trait MinionHandler<T, Err, M: Message<Result = Result<T, Err>>>: Actor + Handler<M> 
where
    T: Send,
    Err: Error + Send
{
}

impl<O, Err, M, T> MinionHandler<O, Err, M> for T 
where
    O: Send,
    Err: Error + Send,
    M: Message<Result = Result<O, Err>>,
    T: Handler<M> + Send
{
}


pub trait MinionAddressExt<A: Actor> {

    fn handled_do_send<T, E, M>(&self, message: M) -> Result<(), Disconnected>
    where
        M: Message<Result = Result<T, E>>,
        T: Send,
        E: Error + Send,
        A: MinionHandler<T, E, M> + Send;
}

impl<A: Actor> MinionAddressExt<A> for Address<A> {
    
    fn handled_do_send<T, E, M>(&self, message: M) -> Result<(), Disconnected>
    where
        M: Message<Result = Result<T, E>>,
        T: Send,
        E: Error + Send,
        A: MinionHandler<T, E, M> + Send {
            Ok(())
        }
}