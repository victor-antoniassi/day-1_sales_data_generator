-- Drop functions if they exist to ensure a clean setup
DROP FUNCTION IF EXISTS simulate_update_sale(simulation_date TIMESTAMP);
DROP FUNCTION IF EXISTS simulate_delete_sale(simulation_date TIMESTAMP);

-- Function to simulate updating a sale by adding a new track to an invoice
CREATE OR REPLACE FUNCTION simulate_update_sale(simulation_date TIMESTAMP)
RETURNS VOID AS $$
DECLARE
    target_invoice_id INT;
    new_track_id INT;
    new_track_price NUMERIC(10, 2);
    new_invoice_line_id INT;
    invoice_total NUMERIC(10, 2);
BEGIN
    -- 1. Select a random invoice from the simulation date (D-1)
    SELECT "InvoiceId" INTO target_invoice_id
    FROM "Invoice"
    WHERE DATE("InvoiceDate") = DATE(simulation_date)
    ORDER BY RANDOM()
    LIMIT 1;

    -- If no invoice is found, do nothing.
    IF target_invoice_id IS NULL THEN
        RAISE NOTICE 'No invoices found for date % to update.', DATE(simulation_date);
        RETURN;
    END IF;

    -- 2. Select a new track that is not already in the invoice
    SELECT "TrackId", "UnitPrice" INTO new_track_id, new_track_price
    FROM "Track"
    WHERE "TrackId" NOT IN (SELECT "TrackId" FROM "InvoiceLine" WHERE "InvoiceId" = target_invoice_id)
    ORDER BY RANDOM()
    LIMIT 1;

    -- If no new track can be added (e.g., invoice already has all tracks), do nothing.
    IF new_track_id IS NULL THEN
        RAISE NOTICE 'No new tracks could be added to InvoiceId %.', target_invoice_id;
        RETURN;
    END IF;

    -- 3. Get the next ID for the InvoiceLine using the sequence
    new_invoice_line_id := nextval('invoice_line_id_seq');

    -- 4. Insert the new item into the invoice
    INSERT INTO "InvoiceLine" ("InvoiceLineId", "InvoiceId", "TrackId", "UnitPrice", "Quantity")
    VALUES (new_invoice_line_id, target_invoice_id, new_track_id, new_track_price, 1);

    -- 5. Recalculate the invoice total
    SELECT SUM("UnitPrice" * "Quantity") INTO invoice_total
    FROM "InvoiceLine"
    WHERE "InvoiceId" = target_invoice_id;

    -- 6. Update the total in the Invoice table
    UPDATE "Invoice"
    SET "Total" = invoice_total
    WHERE "InvoiceId" = target_invoice_id;

    RAISE NOTICE 'Updated InvoiceId % by adding TrackId %.', target_invoice_id, new_track_id;
END;
$$ LANGUAGE plpgsql;

-- Function to simulate deleting a sale
CREATE OR REPLACE FUNCTION simulate_delete_sale(simulation_date TIMESTAMP)
RETURNS VOID AS $$
DECLARE
    target_invoice_id INT;
BEGIN
    -- 1. Select a random invoice from the simulation date (D-1) to delete
    SELECT "InvoiceId" INTO target_invoice_id
    FROM "Invoice"
    WHERE DATE("InvoiceDate") = DATE(simulation_date)
    ORDER BY RANDOM()
    LIMIT 1;

    -- If no invoice is found, do nothing.
    IF target_invoice_id IS NULL THEN
        RAISE NOTICE 'No invoices found for date % to delete.', DATE(simulation_date);
        RETURN;
    END IF;

    -- 2. Delete the invoice lines.
    -- While ON DELETE CASCADE could handle this, being explicit is safer.
    DELETE FROM "InvoiceLine"
    WHERE "InvoiceId" = target_invoice_id;

    -- 3. Delete the invoice itself
    DELETE FROM "Invoice"
    WHERE "InvoiceId" = target_invoice_id;

    RAISE NOTICE 'Deleted InvoiceId % and its lines.', target_invoice_id;
END;
$$ LANGUAGE plpgsql;
