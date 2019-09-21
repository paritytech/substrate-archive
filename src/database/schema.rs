
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

joinable!(inherants -> blocks (hash));
joinable!(signed_extrinsics -> blocks (hash));

allow_tables_to_appear_in_same_query!(
    blocks,
    inherants,
    signed_extrinsics,
);
