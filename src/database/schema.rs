table! {
    accounts (address) {
        address -> Bytea,
        free_balance -> Int8,
        reserved_balance -> Int8,
        account_index -> Bytea,
        nonce -> Int4,
        create_hash -> Bytea,
        created -> Int8,
        updated -> Int8,
        active -> Bool,
    }
}

table! {
    blocks (hash) {
        id -> Int4,
        parent_hash -> Bytea,
        hash -> Bytea,
        block_num -> Int8,
        state_root -> Bytea,
        extrinsics_root -> Bytea,
        time -> Nullable<Timestamptz>,
    }
}

table! {
    inherents (id) {
        id -> Int4,
        hash -> Bytea,
        block_num -> Int8,
        module -> Varchar,
        call -> Varchar,
        parameters -> Nullable<Bytea>,
        in_index -> Int4,
        transaction_version -> Int4,
    }
}

table! {
    signed_extrinsics (transaction_hash) {
        transaction_hash -> Bytea,
        block_num -> Int8,
        hash -> Bytea,
        from_addr -> Bytea,
        to_addr -> Nullable<Bytea>,
        call -> Varchar,
        nonce -> Int4,
        tx_index -> Int4,
        signature -> Bytea,
        transaction_version -> Int4,
    }
}

joinable!(accounts -> blocks (create_hash));
joinable!(inherents -> blocks (hash));
joinable!(signed_extrinsics -> blocks (hash));

allow_tables_to_appear_in_same_query!(accounts, blocks, inherents, signed_extrinsics,);
