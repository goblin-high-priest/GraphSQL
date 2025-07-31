CREATE OR REPLACE FUNCTION floyd_warshall_any()
RETURNS TABLE (src int, dst int, dist numeric) AS
$$
DECLARE
    inf   constant numeric := 1e18;
    n     int;
    v_id_arr  int[];
    v_pos_arr int[];
    D     numeric[][];
    rec   record;
    i int; j int; k int;
BEGIN
    WITH v AS (
        SELECT id,
               row_number() OVER (ORDER BY id) AS rn
        FROM   vertices
    ), maps AS (
        SELECT array_agg(id ORDER BY rn) AS id_arr,
               array_agg(rn ORDER BY id) AS pos_arr
        FROM   v
    )
    SELECT id_arr, pos_arr
      INTO v_id_arr, v_pos_arr
    FROM maps;

    n := array_length(v_id_arr, 1);

    D := array_fill(inf, ARRAY[n,n]);

    FOR i IN 1..n LOOP
        D[i][i] := 0;
    END LOOP;

    FOR rec IN SELECT * FROM edges LOOP
        i := v_pos_arr[rec.src + 1];
        j := v_pos_arr[rec.dst + 1];
        D[i][j] := LEAST(D[i][j], rec.weight);
    END LOOP;

    FOR k IN 1..n LOOP
        FOR i IN 1..n LOOP
            CONTINUE WHEN D[i][k] = inf;
            FOR j IN 1..n LOOP
                IF D[k][j] < inf AND D[i][k] + D[k][j] < D[i][j] THEN
                    D[i][j] := D[i][k] + D[k][j];
                END IF;
            END LOOP;
        END LOOP;
    END LOOP;

    FOR i IN 1..n LOOP
        FOR j IN 1..n LOOP
            src  := v_id_arr[i];
            dst  := v_id_arr[j];
            dist := D[i][j];
            RETURN NEXT;
        END LOOP;
    END LOOP;
END
$$ LANGUAGE plpgsql;

SELECT * FROM floyd_warshall_any()
ORDER BY src, dst;
