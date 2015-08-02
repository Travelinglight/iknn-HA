CREATE OR REPLACE FUNCTION HAwithdraw(tb text) RETURNS void
AS $T1$
DECLARE
    rec record;
    i int;  -- counter
BEGIN
--  delete the extra 3 columns of the target table
    EXECUTE format('ALTER TABLE %s DROP COLUMN ha_id;', tb);

--  drop bins
    FOR rec IN EXECUTE format('SELECT * FROM %s;', tb || '_HATMP;')
    LOOP
        i = 1;
        LOOP
            EXIT WHEN i > rec.nbin;
            EXECUTE format('DROP TABLE habin_%s_%s_%s;', tb, rec.dimension, i::text);
            i := i + 1;
        END LOOP;
    END LOOP;

--  drop the temporary table for Lattices
    EXECUTE format('DROP TABLE %s;', tb || '_HATMP');

--  drop the triggers
    EXECUTE format('DROP TRIGGER %s_hainup ON %s;', tb, tb);
    EXECUTE format('DROP TRIGGER %s_hadel ON %s;', tb, tb);

END
$T1$
language plpgsql;
