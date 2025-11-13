-- =============================================================================
-- SIMULATION FUNCTIONS: Complete SQL setup for D-1 sales data generation
-- =============================================================================
--
-- This file contains all the database logic needed to simulate sales
-- transactions for the D-1 (Day-1) batch data pipeline:
--
-- 1. Sequences for atomic ID generation
-- 2. simulate_new_sale() - Creates new invoices (INSERT operations)
-- 3. simulate_update_sale() - Adds tracks to existing invoices (UPDATE operations)
-- 4. simulate_delete_sale() - Removes invoices (DELETE operations)
--
-- =============================================================================

-- =============================================================================
-- SEQUENCES: Invoice and InvoiceLine ID generation
-- =============================================================================
--
-- Create sequences for atomic, concurrent-safe ID generation.
-- These sequences are initialized to start from the current maximum ID + 1.
-- The IF NOT EXISTS clause ensures idempotency.
--
-- =============================================================================

DO $$
BEGIN
    -- Create sequence for Invoice IDs if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'invoice_id_seq') THEN
        EXECUTE 'CREATE SEQUENCE invoice_id_seq START WITH 1'; -- Start with 1, will be adjusted below
    END IF;
    -- Always restart the sequence to ensure it's greater than MAX("InvoiceId")
    EXECUTE 'ALTER SEQUENCE invoice_id_seq RESTART WITH ' ||
            (SELECT COALESCE(MAX("InvoiceId"), 0) + 1 FROM "Invoice");

    -- Create sequence for InvoiceLine IDs if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'invoice_line_id_seq') THEN
        EXECUTE 'CREATE SEQUENCE invoice_line_id_seq START WITH 1'; -- Start with 1, will be adjusted below
    END IF;
    -- Always restart the sequence to ensure it's greater than MAX("InvoiceLineId")
    EXECUTE 'ALTER SEQUENCE invoice_line_id_seq RESTART WITH ' ||
            (SELECT COALESCE(MAX("InvoiceLineId"), 0) + 1 FROM "InvoiceLine");
END
$$;

-- =============================================================================
-- FUNCTION: simulate_new_sale(p_invoice_date TIMESTAMP WITH TIME ZONE)
-- =============================================================================
--
-- DESCRIPTION:
--   Simulates the creation of a single, complete sales transaction.
--   This function is designed to be called repeatedly by an external script
--   to generate synthetic sales data. It ensures data integrity by
--   encapsulating the entire sale creation process within a single function.
--
-- PARAMETERS:
--   p_invoice_date (TIMESTAMP WITH TIME ZONE): The exact timestamp to be used for the new
--                               invoice. This allows the calling script to
--                               control the temporal distribution of sales.
--
-- RETURNS:
--   TABLE(generated_invoice_id INT, generated_total NUMERIC):
--     - generated_invoice_id: The ID of the newly created invoice.
--     - generated_total: The final calculated total for the invoice.
--
-- LOGIC:
--   1. Selects a random customer from the "Customer" table.
--   2. Generates the next InvoiceId using a SEQUENCE (concurrent-safe).
--   3. Inserts a new record into the "Invoice" table with a placeholder total.
--   4. Randomly decides on a number of items for the invoice (1 to 5).
--   5. Iteratively adds unique tracks to the "InvoiceLine" table, ensuring
--      no duplicate tracks are in the same invoice.
--   6. Accumulates the total price from the selected tracks.
--   7. Updates the "Invoice" record with the correct, final total.
--   8. Returns the new invoice ID and its total for logging purposes.
--
-- =============================================================================

CREATE OR REPLACE FUNCTION simulate_new_sale(p_invoice_date TIMESTAMP WITH TIME ZONE)
RETURNS TABLE(generated_invoice_id INT, generated_total NUMERIC) AS $$
DECLARE
    -- Variable declarations
    v_customer_id INT;
    v_invoice_id INT;
    v_total_price NUMERIC := 0;
    v_num_items INT;
    i INT;
    -- Array to prevent adding the same track twice to the same invoice
    v_track_ids_in_invoice INT[] := '{}';
    v_track_info RECORD;
BEGIN
    -- Step 1: Select a random customer
    SELECT "CustomerId" INTO v_customer_id
    FROM "Customer" ORDER BY RANDOM() LIMIT 1;

    -- Step 2: Get the next sequential InvoiceId using SEQUENCE (concurrent-safe)
    v_invoice_id := nextval('invoice_id_seq');

    -- Step 3: Insert the main invoice record.
    -- The total is temporarily set to 0.00 and will be updated at the end.
    INSERT INTO "Invoice" (
        "InvoiceId", "CustomerId", "InvoiceDate", "BillingAddress",
        "BillingCity", "BillingState", "BillingCountry", "BillingPostalCode", "Total"
    )
    SELECT
        v_invoice_id,
        v_customer_id,
        p_invoice_date,
        c."Address",
        c."City",
        c."State",
        c."Country",
        c."PostalCode",
        0.00 -- Placeholder total
    FROM "Customer" c WHERE c."CustomerId" = v_customer_id;

    -- Step 4: Determine a random number of items for this sale (e.g., 1 to 5)
    v_num_items := floor(random() * 5) + 1;

    -- Step 5: Loop to insert the corresponding invoice items (InvoiceLine)
    FOR i IN 1..v_num_items LOOP
        -- Select a random track that has not already been added to this invoice
        SELECT "TrackId", "UnitPrice"
        INTO v_track_info
        FROM "Track"
        WHERE "TrackId" NOT IN (SELECT unnest(v_track_ids_in_invoice))
        ORDER BY RANDOM()
        LIMIT 1;

        -- Store the added track ID to prevent duplicates in this invoice
        v_track_ids_in_invoice := array_append(v_track_ids_in_invoice, v_track_info."TrackId");

        -- Insert the new invoice item using SEQUENCE for ID generation
        INSERT INTO "InvoiceLine" (
            "InvoiceLineId", "InvoiceId", "TrackId", "UnitPrice", "Quantity"
        )
        VALUES (
            nextval('invoice_line_id_seq'), -- SEQUENCE for concurrent-safe ID generation
            v_invoice_id,
            v_track_info."TrackId",
            v_track_info."UnitPrice",
            1 -- Quantity is always 1 as per the Chinook data model
        );

        -- Accumulate the total price for the final update
        v_total_price := v_total_price + v_track_info."UnitPrice";
    END LOOP;

    -- Step 6: Update the invoice with the correct, calculated total
    UPDATE "Invoice"
    SET "Total" = v_total_price
    WHERE "InvoiceId" = v_invoice_id;

    -- Step 7: Return the generated Invoice ID and Total for logging
    RETURN QUERY SELECT v_invoice_id, v_total_price;

END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FUNCTION: simulate_update_sale(simulation_date TIMESTAMP WITH TIME ZONE)
-- =============================================================================
--
-- DESCRIPTION:
--   Simulates updating an existing invoice by adding a new track to it.
--   This represents a customer adding an additional item to their order.
--   Operates on invoices within a 90-day window.
--
-- PARAMETERS:
--   simulation_date (TIMESTAMP WITH TIME ZONE): The timestamp for the modification.
--
-- RETURNS:
--   INT: The ID of the updated invoice, or NULL if no invoice was updated.
--
-- =============================================================================

CREATE OR REPLACE FUNCTION simulate_update_sale(simulation_date TIMESTAMP WITH TIME ZONE)
RETURNS INT AS $$
DECLARE
    target_invoice_id INT;
    new_track_id INT;
    new_track_price NUMERIC(10, 2);
BEGIN
    -- 1. Select a random, non-cancelled invoice from the last 90 days
    SELECT "InvoiceId" INTO target_invoice_id
    FROM "Invoice"
    WHERE "InvoiceDate" >= (simulation_date::date - INTERVAL '90 days')
      AND "Total" > 0
    ORDER BY RANDOM()
    LIMIT 1;

    IF target_invoice_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- 2. Select a new track that is not already in the invoice
    SELECT "TrackId", "UnitPrice" INTO new_track_id, new_track_price
    FROM "Track"
    WHERE "TrackId" NOT IN (SELECT "TrackId" FROM "InvoiceLine" WHERE "InvoiceId" = target_invoice_id)
    ORDER BY RANDOM()
    LIMIT 1;

    IF new_track_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- 3. Insert the new item into the invoice
    INSERT INTO "InvoiceLine" ("InvoiceLineId", "InvoiceId", "TrackId", "UnitPrice", "Quantity")
    VALUES (nextval('invoice_line_id_seq'), target_invoice_id, new_track_id, new_track_price, 1);

    -- 4. Recalculate and update the total in the Invoice table
    UPDATE "Invoice"
    SET "Total" = (SELECT SUM("UnitPrice" * "Quantity") FROM "InvoiceLine" WHERE "InvoiceId" = target_invoice_id)
    WHERE "InvoiceId" = target_invoice_id;

    RETURN target_invoice_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- FUNCTION: simulate_delete_sale(simulation_date TIMESTAMP WITH TIME ZONE)
-- =============================================================================
--
-- DESCRIPTION:
--   Simulates deleting (canceling) an invoice completely.
--   Operates on invoices within a 90-day window.
--
-- PARAMETERS:
--   simulation_date (TIMESTAMP WITH TIME ZONE): The timestamp for the modification.
--
-- RETURNS:
--   INT: The ID of the deleted invoice, or NULL if no invoice was deleted.
--
-- =============================================================================

CREATE OR REPLACE FUNCTION simulate_delete_sale(simulation_date TIMESTAMP WITH TIME ZONE)
RETURNS INT AS $$
DECLARE
    target_invoice_id INT;
BEGIN
    -- 1. Select a random, non-cancelled invoice from the last 90 days to delete
    SELECT "InvoiceId" INTO target_invoice_id
    FROM "Invoice"
    WHERE "InvoiceDate" >= (simulation_date::date - INTERVAL '90 days')
      AND "Total" > 0
    ORDER BY RANDOM()
    LIMIT 1;

    IF target_invoice_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- 2. Delete the invoice lines
    DELETE FROM "InvoiceLine"
    WHERE "InvoiceId" = target_invoice_id;

    -- 3. Delete the invoice itself
    DELETE FROM "Invoice"
    WHERE "InvoiceId" = target_invoice_id;

    RETURN target_invoice_id;
END;
$$ LANGUAGE plpgsql;