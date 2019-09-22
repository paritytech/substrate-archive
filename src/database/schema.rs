table! {
    accounts (address) {
        address -> Bytea,
        free_balance -> Int4,
        reserved_balance -> Int4,
        account_index -> Bytea,
        nonce -> Int4,
        create_hash -> Bytea,
        created -> Bytea,
        updated -> Bytea,
        active -> Bool,
    }
}

table! {
    blocks (hash) {
        parent_hash -> Bytea,
        hash -> Bytea,
        block -> Bytea,
        state_root -> Bytea,
        extrinsics_root -> Bytea,
        time -> Nullable<Timestamp>,
    }
}

table! {
    inherants (id) {
        id -> Int4,
        hash -> Bytea,
        block -> Bytea,
        module -> Varchar,
        call -> Varchar,
        success -> Bool,
        in_index -> Int4,
    }
}

table! {
    signed_extrinsics (transaction_hash) {
        transaction_hash -> Bytea,
        block -> Bytea,
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
