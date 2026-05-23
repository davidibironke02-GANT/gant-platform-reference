-- 7.4 Row-Level Security
-- PostgreSQL RLS implementation for buyer and farmer data isolation
-- Database engine is the enforcement authority
-- Application-level scoping is the second layer
-- Even a malformed application query returns only rows
-- belonging to the authenticated user

-- ── Enable RLS on core tables ─────────────────────────────────────

ALTER TABLE trades
    ENABLE ROW LEVEL SECURITY;

ALTER TABLE dsre_log
    ENABLE ROW LEVEL SECURITY;

ALTER TABLE trade_batchlot
    ENABLE ROW LEVEL SECURITY;

ALTER TABLE logistics_financing
    ENABLE ROW LEVEL SECURITY;

ALTER TABLE escrow
    ENABLE ROW LEVEL SECURITY;

ALTER TABLE batches
    ENABLE ROW LEVEL SECURITY;

ALTER TABLE dsre_advisories
    ENABLE ROW LEVEL SECURITY;


-- ── Buyer isolation policies ──────────────────────────────────────
-- Every query under buyer_role automatically appends a BUID filter
-- enforced by the database engine before any rows are returned
-- regardless of the query's own WHERE clause

CREATE POLICY buyer_trade_isolation
    ON trades
    FOR ALL
    TO buyer_role
    USING (buid = current_setting('app.current_buid')::text);

-- DSRE log isolation joins through TRADE to enforce BUID scope
CREATE POLICY buyer_dsre_isolation
    ON dsre_log
    FOR ALL
    TO buyer_role
    USING (
        EXISTS (
            SELECT 1 FROM trades
            WHERE trades.trade_id = dsre_log.trade_id
            AND   trades.buid     = current_setting('app.current_buid')::text
        )
    );

CREATE POLICY buyer_advisory_isolation
    ON dsre_advisories
    FOR ALL
    TO buyer_role
    USING (
        EXISTS (
            SELECT 1 FROM trades
            WHERE trades.trade_id        = dsre_advisories.trade_id
            AND   trades.buid            = current_setting('app.current_buid')::text
        )
    );

CREATE POLICY buyer_trade_batchlot_isolation
    ON trade_batchlot
    FOR ALL
    TO buyer_role
    USING (
        EXISTS (
            SELECT 1 FROM trades
            WHERE trades.trade_id    = trade_batchlot.trade_id
            AND   trades.buid        = current_setting('app.current_buid')::text
        )
    );

CREATE POLICY buyer_logistics_isolation
    ON logistics_financing
    FOR ALL
    TO buyer_role
    USING (
        EXISTS (
            SELECT 1 FROM trades
            WHERE trades.trade_id = logistics_financing.trade_id
            AND   trades.buid     = current_setting('app.current_buid')::text
        )
    );

CREATE POLICY buyer_escrow_isolation
    ON escrow
    FOR ALL
    TO buyer_role
    USING (
        EXISTS (
            SELECT 1 FROM trades
            WHERE trades.trade_id = escrow.trade_id
            AND   trades.buid     = current_setting('app.current_buid')::text
        )
    );


-- ── Farmer isolation policy ───────────────────────────────────────
-- Farmer sees only their own batches identified by FUID

CREATE POLICY farmer_batch_isolation
    ON batches
    FOR ALL
    TO farmer_role
    USING (fuid = current_setting('app.current_fuid')::text);


-- ── Admin bypass policies ─────────────────────────────────────────
-- Admin role has full table access for operational oversight
-- Every admin access event is logged to the audit trail independently

CREATE POLICY admin_trade_access
    ON trades
    FOR ALL
    TO admin_role
    USING (true);

CREATE POLICY admin_dsre_access
    ON dsre_log
    FOR ALL
    TO admin_role
    USING (true);

CREATE POLICY admin_batch_access
    ON batches
    FOR ALL
    TO admin_role
    USING (true);

CREATE POLICY admin_escrow_access
    ON escrow
    FOR ALL
    TO admin_role
    USING (true);

CREATE POLICY admin_logistics_access
    ON logistics_financing
    FOR ALL
    TO admin_role
    USING (true);


-- ── Application-level session configuration ───────────────────────
-- This block runs within the database transaction before any
-- data query executes. API Gateway extracts the authenticated
-- identity from the validated Cognito JWT token and passes it
-- to the backend service which sets the session variable
-- before issuing any query.

-- For buyer requests:
-- SET LOCAL app.current_buid TO '<buid_from_jwt_claims>';

-- For farmer requests:
-- SET LOCAL app.current_fuid TO '<fuid_from_jwt_claims>';

-- SET LOCAL scopes the variable to the current transaction.
-- It is automatically cleared when the transaction ends.
-- No cross-request identity bleed is possible.


-- ── Verification queries ──────────────────────────────────────────
-- Run these as buyer_role with app.current_buid set to confirm
-- policies are enforcing correctly before production deployment

-- Should return only trades belonging to the set BUID:
-- SET LOCAL app.current_buid TO 'BUID-TEST-001';
-- SELECT trade_id, buid, trade_status FROM trades;

-- Should return empty if no trades exist for this BUID:
-- SET LOCAL app.current_buid TO 'BUID-NONEXISTENT';
-- SELECT COUNT(*) FROM trades;
-- Expected: 0
