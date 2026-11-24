-- Per-subject salt for on-chain anchor identifiers (decentralized variant only).
--
-- WHY. ConsentAnchor emits (consentId, personId, payloadHash, action). Writing the raw
-- UUIDs put a permanent, unerasable identifier on an append-only ledger: the payload was
-- hashed, the identifiers were not. A UUIDv4 carries ~122 bits of entropy, so a plain
-- hash is not brute-forceable, but it is still LINKABLE -- the same person always maps to
-- the same value, so every anchor for one subject stays joined forever, and Art. 17
-- erasure cannot reach it.
--
-- WHAT THIS ENABLES. Anchors now carry HMAC-SHA256(salt_person, uuid). The salt lives
-- here, off-chain, and is destroyed when the person is erased. After that the on-chain
-- values cannot be recomputed or grouped: crypto-shredding. The ledger keeps its
-- integrity proofs; the identifiers stop being linkable to a person.
--
-- This table belongs to the anchoring superset, not the shared base schema, so the
-- same-contract comparison across the five variants is unaffected.
CREATE TABLE IF NOT EXISTS person_anchor_salt (
    person_id  UUID PRIMARY KEY,
    salt       BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE person_anchor_salt IS
    'Per-subject HMAC salt for on-chain anchor identifiers. Destroying a row renders that subject''s existing on-chain anchors unlinkable (Art. 17 crypto-shredding).';
