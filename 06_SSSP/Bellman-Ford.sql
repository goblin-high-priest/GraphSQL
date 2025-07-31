CREATE OR REPLACE FUNCTION bellman_ford(start_vertex int)
RETURNS TABLE(vertex int, distance numeric, predecessor int) AS
$$
DECLARE
    v_cnt  int;
    i      int;
BEGIN
    -- 临时表
    CREATE TEMP TABLE dist (
        vertex       int PRIMARY KEY,
        distance     numeric,
        predecessor  int
    ) ON COMMIT DROP;

    -- 初始化
    INSERT INTO dist(vertex, distance)
    SELECT id, CASE WHEN id = start_vertex THEN 0 ELSE 1e18 END
    FROM vertices;

    SELECT COUNT(*) INTO v_cnt FROM vertices;

    -- |V|-1 轮松弛
    FOR i IN 1 .. v_cnt-1 LOOP
        UPDATE dist d
        SET distance     = s.distance + e.weight,
            predecessor  = e.source
        FROM dist s
        JOIN edges e ON e.source = s.vertex
        WHERE e.target = d.vertex
          AND s.distance + e.weight < d.distance;

        -- 若本轮无更新，可提前跳出
        GET DIAGNOSTICS v_cnt = ROW_COUNT;
        EXIT WHEN v_cnt = 0;
    END LOOP;

    -- 在 PL/pgSQL 最后加：
    IF EXISTS (
        SELECT 1
        FROM dist d
        JOIN edges e ON e.source = d.vertex
        JOIN dist d2 ON d2.vertex = e.target
        WHERE d.distance + e.weight < d2.distance
    ) THEN
        RAISE EXCEPTION 'Graph contains a negative-weight cycle – Bellman–Ford aborted';
    END IF;


    RETURN QUERY SELECT * FROM dist;
END;
$$ LANGUAGE plpgsql VOLATILE;

SELECT * FROM bellman_ford(1) ORDER BY vertex;

