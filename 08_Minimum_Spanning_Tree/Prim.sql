CREATE OR REPLACE FUNCTION prim_mst(start_id INT DEFAULT NULL)
RETURNS TABLE(src INT, dst INT, weight NUMERIC) AS
$$
DECLARE
    v_inf       CONSTANT NUMERIC := 1e18;

    v_ids       INT[];     -- row → vertex_id
    v_pos       INT[];     -- vertex_id → row
    v_n         INT;

    v_key       NUMERIC[]; -- min edge weight to MST
    v_parent    INT[];     -- parent row
    v_done      BOOLEAN[];

    r      INT;
    min_r  INT;
    min_w  NUMERIC;
    nb_r   INT;
    nb_w   NUMERIC;
BEGIN

    WITH v AS (
        SELECT id, row_number() OVER (ORDER BY id) AS rn
        FROM vertices
    ), m AS (
        SELECT array_agg(id ORDER BY rn) AS col_ids,
               array_agg(rn ORDER BY id) AS col_pos
        FROM v
    )
    SELECT col_ids, col_pos
      INTO  v_ids,  v_pos
    FROM  m;

    v_n := array_length(v_ids, 1);
    IF v_n IS NULL THEN
        RAISE EXCEPTION 'vertices table is empty';
    END IF;

    v_key    := array_fill(v_inf      , ARRAY[v_n]);
    v_parent := array_fill(NULL::INT  , ARRAY[v_n]);
    v_done   := array_fill(FALSE      , ARRAY[v_n]);

    IF start_id IS NULL THEN
        start_id := v_ids[1];
    END IF;
    r := v_pos[start_id];
    v_key[r] := 0;

    FOR _step IN 1..v_n LOOP
        min_w := v_inf;  min_r := NULL;
        FOR r IN 1..v_n LOOP
            IF NOT v_done[r] AND v_key[r] < min_w THEN
                min_w := v_key[r];  min_r := r;
            END IF;
        END LOOP;
        EXIT WHEN min_r IS NULL;          -- 不连通

        v_done[min_r] := TRUE;

        IF v_parent[min_r] IS NOT NULL THEN
            src    := v_ids[v_parent[min_r]];
            dst    := v_ids[min_r];
            weight := v_key[min_r];
            RETURN NEXT;
        END IF;

        /* 更新邻接点 */
        FOR nb_r, nb_w IN
            SELECT CASE WHEN e.u = v_ids[min_r]
                        THEN v_pos[e.v]
                        ELSE v_pos[e.u] END,
                   e.w
            FROM edges e
            WHERE e.u = v_ids[min_r] OR e.v = v_ids[min_r]
        LOOP
            IF NOT v_done[nb_r] AND nb_w < v_key[nb_r] THEN
                v_key[nb_r]    := nb_w;
                v_parent[nb_r] := min_r;
            END IF;
        END LOOP;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- 默认从最小 id 顶点开始
SELECT * FROM prim_mst()
ORDER BY weight;

-- 指定从顶点 2 (B) 开始
SELECT * FROM prim_mst(2);
