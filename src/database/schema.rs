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


table! {
    accounts (address) {
        address -> Bytea,
        free_balance -> Int4,
        reserved_balance -> Int4,
        account_index -> Bytea,
        nonce -> Int4,
        create_hash -> Bytea,
        created -> Int4,
        updated -> Int4,
        active -> Bool,
    }
}

table! {
    blocks (hash) {
        parent_hash -> Bytea,
        hash -> Bytea,
        block -> Int4,
        state_root -> Bytea,
        extrinsics_root -> Bytea,
        time -> Nullable<Timestamp>,
    }
}

table! {
    inherants (id) {
        id -> Int4,
        hash -> Bytea,
        block -> Int4,
        module -> Varchar,
        call -> Varchar,
        success -> Bool,
        in_index -> Int4,
    }
}

table! {
    signed_extrinsics (transaction_hash) {
        transaction_hash -> Bytea,
        block -> Int4,
        hash -> Bytea,
        from_addr -> Bytea,
        to_addr -> Nullable<Bytea>,
        call -> Varchar,
        success -> Bool,
        nonce -> Int4,
        tx_index -> Int4,
        signature -> Bytea,
    }
}

joinable!(accounts -> blocks (create_hash));
joinable!(inherants -> blocks (hash));
joinable!(signed_extrinsics -> blocks (hash));

allow_tables_to_appear_in_same_query!(
    accounts,
    blocks,
    inherants,
    signed_extrinsics,
);
