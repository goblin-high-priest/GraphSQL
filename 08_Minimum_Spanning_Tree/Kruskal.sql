CREATE OR REPLACE FUNCTION kruskal_mst()
RETURNS TABLE(src INT, dst INT, weight NUMERIC) AS
$$
DECLARE
    id_arr  INT[];   -- row_number → vertex_id   (长度 n)
    pos_arr INT[];   -- vertex_id  → row_number  (下标 = id，本例 id 连续)

    n       INT;     -- 顶点数
    parent  INT[];   -- 并查集父指针
    rank    INT[];   -- 按秩合并

    e       RECORD;  -- 遍历边
    r1 INT; r2 INT;  -- 两端根
BEGIN

    WITH v AS (
        SELECT id,
               row_number() OVER (ORDER BY id) AS rn
        FROM   vertices
    ), m AS (
        SELECT array_agg(id ORDER BY rn) AS ids,
               array_agg(rn ORDER BY id) AS pos
        FROM   v
    )
    SELECT ids, pos
      INTO  id_arr, pos_arr
    FROM  m;

    n := array_length(id_arr, 1);
    IF n IS NULL THEN
        RAISE EXCEPTION 'vertices 表为空';
    END IF;

    parent := array(SELECT generate_series(1, n));  -- 1,2,3,...
    rank   := array_fill(0, ARRAY[n]);

    FOR e IN
        SELECT u, v, w
        FROM   edges
        ORDER  BY w
    LOOP
        /* find(u) */
        r1 := pos_arr[e.u];           -- id → 行号
        WHILE parent[r1] <> r1 LOOP
            parent[r1] := parent[parent[r1]]; -- 路径压缩
            r1 := parent[r1];
        END LOOP;

        /* find(v) */
        r2 := pos_arr[e.v];
        WHILE parent[r2] <> r2 LOOP
            parent[r2] := parent[parent[r2]];
            r2 := parent[r2];
        END LOOP;

        IF r1 <> r2 THEN
            -- union by rank
            IF rank[r1] < rank[r2] THEN
                parent[r1] := r2;
            ELSIF rank[r1] > rank[r2] THEN
                parent[r2] := r1;
            ELSE
                parent[r2] := r1;
                rank[r1] := rank[r1] + 1;
            END IF;

            src    := e.u;
            dst    := e.v;
            weight := e.w;
            RETURN NEXT;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

SELECT * FROM kruskal_mst()
ORDER BY weight;
