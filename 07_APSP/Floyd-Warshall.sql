CREATE OR REPLACE FUNCTION floyd_warshall_any()
RETURNS TABLE (src int, dst int, dist numeric) AS
$$
DECLARE
    inf   constant numeric := 1e18;
    n     int;            -- 顶点数

    -- 两个数组映射；把变量名改掉避免与列名冲突
    v_id_arr  int[];      -- row_number → vertex_id      (下标 1..n)
    v_pos_arr int[];      -- vertex_id+1 → row_number    (让顶点 0 也能查到)

    D     numeric[][];    -- 距离矩阵
    rec   record;
    i int; j int; k int;
BEGIN
    ------------------------------------------------------------------
    -- ① 生成映射表
    ------------------------------------------------------------------
    WITH v AS (
        SELECT id,
               row_number() OVER (ORDER BY id) AS rn
        FROM   vertices
    ), maps AS (
        SELECT array_agg(id ORDER BY rn) AS id_arr,     -- row → id
               array_agg(rn ORDER BY id) AS pos_arr     -- id  → row
        FROM   v
    )
    SELECT id_arr, pos_arr
      INTO v_id_arr, v_pos_arr
    FROM maps;

    n := array_length(v_id_arr, 1);       -- 实际顶点个数

    ------------------------------------------------------------------
    -- ② 初始化距离矩阵
    ------------------------------------------------------------------
    D := array_fill(inf, ARRAY[n,n]);

    FOR i IN 1..n LOOP
        D[i][i] := 0;
    END LOOP;

    -- 写入边权
    FOR rec IN SELECT * FROM edges LOOP
        i := v_pos_arr[rec.src + 1];      -- 顶点 0 也能定位到下标 1
        j := v_pos_arr[rec.dst + 1];
        D[i][j] := LEAST(D[i][j], rec.weight);
    END LOOP;

    ------------------------------------------------------------------
    -- ③ Floyd-Warshall  O(n³)
    ------------------------------------------------------------------
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

    ------------------------------------------------------------------
    -- ④ 输出结果：赋值 → RETURN NEXT
    ------------------------------------------------------------------
    FOR i IN 1..n LOOP
        FOR j IN 1..n LOOP
            src  := v_id_arr[i];
            dst  := v_id_arr[j];
            dist := D[i][j];
            RETURN NEXT;         -- 不带参数，所有版本都兼容
        END LOOP;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- 直接跑
SELECT * FROM floyd_warshall_any()
ORDER BY src, dst;
