-- =============================================================================
-- UPDATE HISTORICAL DATA: Align Chinook historical sales to D-2
-- =============================================================================
--
-- DESCRIPTION:
--   This script updates the historical Chinook invoice data (originally from
--   2009-2013) to align with the D-1 simulation workflow. It moves all
--   historical sales so that the most recent one ends at the end of D-2
--   (day before yesterday), leaving D-1 as a "blank canvas" for new synthetic
--   data generation.
--
-- IDEMPOTENCY:
--   This script is safe to run multiple times. It checks if the data has
--   already been updated and skips the operation if so.
--
-- USAGE:
--   Run this script once during initial setup:
--   neonctl connection-string | xargs -I {} psql {} -f update_historical_data.sql
--
-- =============================================================================

DO $$
DECLARE
    v_max_invoice_date TIMESTAMP;
    v_target_date TIMESTAMP;
    v_interval_to_add INTERVAL;
    v_affected_rows INT;
BEGIN
    -- Calculate the target date: end of day D-2
    v_target_date := (CURRENT_DATE - INTERVAL '2 day' + INTERVAL '23:59:59');

    -- Get the current maximum invoice date
    SELECT MAX("InvoiceDate") INTO v_max_invoice_date FROM "Invoice";

    -- Check if update is needed (idempotency)
    IF v_max_invoice_date >= (CURRENT_DATE - INTERVAL '3 day') THEN
        RAISE NOTICE '===========================================================';
        RAISE NOTICE 'Historical data already aligned (max date: %).', v_max_invoice_date;
        RAISE NOTICE 'Skipping update to maintain data integrity.';
        RAISE NOTICE 'If you need to re-run this, the data is already in the correct range.';
        RAISE NOTICE '===========================================================';
        RETURN;
    END IF;

    -- Calculate the interval to add
    v_interval_to_add := v_target_date - v_max_invoice_date;

    RAISE NOTICE '===========================================================';
    RAISE NOTICE 'UPDATING HISTORICAL INVOICE DATES';
    RAISE NOTICE '===========================================================';
    RAISE NOTICE 'Current max date: %', v_max_invoice_date;
    RAISE NOTICE 'Target max date:  %', v_target_date;
    RAISE NOTICE 'Interval to add:  %', v_interval_to_add;
    RAISE NOTICE '';

    -- Perform the update
    WITH UpdatedInvoices AS (
        UPDATE "Invoice"
        SET "InvoiceDate" = "InvoiceDate" + v_interval_to_add
        RETURNING "InvoiceId"
    )
    SELECT COUNT(*) INTO v_affected_rows FROM UpdatedInvoices;

    RAISE NOTICE 'Updated % invoice records.', v_affected_rows;
    RAISE NOTICE '';

    -- Verify the update
    SELECT MAX("InvoiceDate") INTO v_max_invoice_date FROM "Invoice";
    RAISE NOTICE 'New max invoice date: %', v_max_invoice_date;
    RAISE NOTICE '';
    RAISE NOTICE 'SUCCESS: Historical data aligned to D-2.';
    RAISE NOTICE 'D-1 (%) is now available for synthetic data generation.', CURRENT_DATE - INTERVAL '1 day';
    RAISE NOTICE '===========================================================';

END $$;
