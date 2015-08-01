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
    EXECUTE format('CREATE TABLE %s(dimension varchar(255), nbin int, nobj int);', tb || '_HATMP');

    cnt := 0;
    FOR recCol IN EXECUTE format('SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s', quote_nullable(tb))
    LOOP
        IF recCol.column_name = 'ha_id' THEN CONTINUE; END IF;
        cnt := cnt + 1;
        -- update the _HATMP table;
        EXECUTE format('INSERT INTO %s_hatmp VALUES(%s, %s, (SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL));', tb, quote_nullable(recCol.column_name), nBin[cnt]::text, tb, recCol.column_name);

        -- create bins
        i := 1;
        LOOP
            EXIT WHEN i > nBin[cnt];
            EXECUTE format('CREATE table habin_%s_%s_%s(val %s, ha_id int);', tb, recCol.column_name, i::text, recCol.data_type);
            EXECUTE format('CREATE INDEX ha_%s_%s_%s ON habin_%s_%s_%s USING BTREE (val);', tb, recCol.column_name, i::text, tb, recCol.column_name, i::text);
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

    EXECUTE format('
    CREATE OR REPLACE FUNCTION HA_%s_triInUp() RETURNS TRIGGER 
    AS $T2$
    DECLARE
        par record;
        recl record;
        recr record;
        nbin int;
        nobj int;
        ist int;
    BEGIN
        FOR par IN SELECT (each(hstore(NEW))).*
        LOOP
            IF par.key = ''ha_id'' THEN CONTINUE; END IF;
            IF par.value IS NOT NULL THEN
                EXECUTE format(''SELECT nbin FROM %s_hatmp WHERE dimension = %%s'', quote_nullable(par.key)) INTO nbin;
                EXECUTE format(''SELECT nobj FROM %s_hatmp WHERE dimension = %%s'', quote_nullable(par.key)) INTO nobj;
                ist = nobj %% nbin + 1;
    
                IF ist < nbin AND nobj > nbin THEN  -- get the largest of the left bin
                    EXECUTE format(''SELECT * FROM habin_%s_%%s_%%s ORDER BY val LIMIT 1'', par.key, (ist+1)::text) INTO recr;
                    LOOP
                        EXIT WHEN par.value::float <= recr.val::float;
                        EXIT WHEN recr IS NULL;
                        EXECUTE format(''INSERT INTO habin_%s_%%s_%%s VALUES(%%s, %%s)'', par.key, ist::text, recr.val, recr.ha_id);
                        EXECUTE format(''DELETE FROM habin_%s_%%s_%%s WHERE ha_id = %%s'', par.key, (ist+1)::text, recr.ha_id);
                        ist := ist + 1;
                        IF ist = nbin THEN EXIT; END IF;
                        EXECUTE format(''SELECT %s AS val, ha_id FROM habin_%s_%%s_%%s ORDER BY %%s DESC LIMIT 1'', par.key, par.key, (ist+1)::text, par.key) INTO recr;
                    END LOOP;
                END IF;
                IF ist > 1 THEN     -- get the smallest of the right bin
                    EXECUTE format(''SELECT * FROM habin_%s_%%s_%%s ORDER BY val DESC LIMIT 1'', par.key, (ist-1)::text) INTO recl;
                    LOOP
                        EXIT WHEN par.value::float >= recl.val::float;
                        EXECUTE format(''INSERT INTO habin_%s_%%s_%%s VALUES(%%s, %%s)'', par.key, ist::text, recl.val, recl.ha_id);
                        EXECUTE format(''DELETE FROM habin_%s_%%s_%%s WHERE ha_id = %%s'', par.key, (ist-1)::text, recl.ha_id);
                        ist := ist - 1;
                        IF ist = 1 THEN EXIT; END IF;
                        EXECUTE format(''SELECT %s AS val, ha_id FROM habin_%s_%%s_%%s ORDER BY %%s DESC LIMIT 1'', par.key, par.key, (ist-1)::text, par.key) INTO recl;
                    END LOOP;
                END IF;
                EXECUTE format(''INSERT INTO habin_%s_%%s_%%s VALUES(%%s, %%s)'', par.key, ist::text, par.value, NEW.ha_id);
                EXECUTE format(''UPDATE %s_hatmp SET nobj = nobj + 1 where dimension = %%s'', quote_nullable(par.key));
            END IF;
        END LOOP;
        RETURN NEW;
    END
    $T2$ LANGUAGE plpgsql;', tb, tb, tb, tb, tb, tb, tb, tb, tb, tb, tb, tb, tb, tb, tb);

    EXECUTE format('
    CREATE TRIGGER %s_HAinup AFTER INSERT OR UPDATE ON %s
    FOR EACH ROW EXECUTE PROCEDURE HA_%s_triInUp();', tb, tb, tb); 

END
$$
language plpgsql;
