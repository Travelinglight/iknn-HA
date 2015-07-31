CREATE EXTENSION hstore;

CREATE OR REPLACE FUNCTION HAinit(tb text, VARIADIC nBin int[]) RETURNS void
AS $$
DECLARE
    cnt int;    -- count through records selected
    i int;      -- counter
    j int;      -- counter
    nRec int;   -- record how many records returned

    recCol record;  -- for looping through selects
    recRec record;  -- for looping through tuples
    nDiv int;   -- for updating bins
    nMod int;   -- for updating bins
BEGIN
--  add the ha_id column to the original table
    EXECUTE format('ALTER TABLE %s ADD COLUMN ha_id SERIAL', tb);

--  create a table to store number of bins on each column
    EXECUTE format('CREATE TABLE %s(dimension varchar(255), nbin int);', tb || '_HATMP');

    cnt := 0;
    FOR recCol IN EXECUTE format('SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s', quote_nullable(tb))
    LOOP
        IF recCol.column_name = 'ha_id' THEN CONTINUE; END IF;
        cnt := cnt + 1;
        -- update the _HATMP table;
        EXECUTE format('INSERT INTO %s_hatmp VALUES(%s, %s);', tb, quote_nullable(recCol.column_name), nBin[cnt]::text);

        -- create bins
        i := 1;
        LOOP
            EXIT WHEN i > nBin[cnt];
            EXECUTE format('CREATE table habin_%s_%s_%s(%s %s, ha_id int);', tb, recCol.column_name, i::text, recCol.column_name, recCol.data_type);
            EXECUTE format('CREATE INDEX ha_%s_%s_%s ON habin_%s_%s_%s USING BTREE (%s);', tb, recCol.column_name, i::text, tb, recCol.column_name, i::text, recCol.column_name);
            i := i + 1;
        END LOOP;

        -- update bins
        EXECUTE format('SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL;', tb, recCol.column_name) INTO nRec;
        nDiv = nRec / nBin[cnt];
        nMod = nRec % nBin[cnt];
        i := 1;
        j := 0;
        FOR recRec IN EXECUTE format('SELECT %s AS val, ha_id FROM %s WHERE %s IS NOT NULL ORDER BY %s', recCol.column_name, tb, recCol.column_name, recCol.column_name)
        LOOP
            j := j + 1;
            EXECUTE format('INSERT INTO habin_%s_%s_%s VALUES(%s, %s);', tb, recCol.column_name, i::text, recRec.val, recRec.ha_id);
            IF j <= (nDiv + 1) * nMod THEN
                IF j % (nDiv + 1) = 0 THEN
                    i := i + 1;
                END IF;
            ELSE
                IF (j - (nDiv + 1) * nMod) % nDiv = 0 THEN
                    i := i + 1;
                END IF;
            END IF;
        END LOOP;
    END LOOP;

END
$$
language plpgsql;
