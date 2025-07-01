CREATE OR REPLACE FUNCTION run_hits()
RETURNS VOID AS $$
DECLARE
    diff FLOAT := 1.0;
    margin FLOAT := 0.0001;
BEGIN
    LOOP
        -- Step 1: 备份当前值
        UPDATE nodes
        SET prev_auth = curr_auth,
            prev_hub = curr_hub;

        -- Step 2: 计算 curr_auth（入边的 curr_hub 之和）
        WITH authority AS (
            SELECT
                n.nodeid,
                COALESCE(x.total_hub, 0) AS authority_score
            FROM nodes n
            LEFT JOIN (
                SELECT
                    e.targetnodeid AS nodeid,
                    SUM(COALESCE(n.curr_hub, 0)) AS total_hub
                FROM edges e
                JOIN nodes n ON e.sourcenodeid = n.nodeid
                GROUP BY e.targetnodeid
            ) x ON n.nodeid = x.nodeid
        )
        UPDATE nodes n
        SET curr_auth = a.authority_score
        FROM authority a
        WHERE n.nodeid = a.nodeid;

        -- Step 3: 归一化 curr_auth
        WITH total AS (
            SELECT SUM(curr_auth) AS s FROM nodes
        )
        UPDATE nodes
        SET curr_auth = CASE
            WHEN total.s = 0 THEN 0
            ELSE ROUND(curr_auth / total.s, 6)
        END
        FROM total;

        -- Step 4: 计算 curr_hub（出边的 curr_auth 之和）
        WITH hub AS (
            SELECT
                n.nodeid,
                COALESCE(x.total_auth, 0) AS hub_score
            FROM nodes n
            LEFT JOIN (
                SELECT
                    e.sourcenodeid AS nodeid,
                    SUM(COALESCE(n.curr_auth, 0)) AS total_auth
                FROM edges e
                JOIN nodes n ON e.targetnodeid = n.nodeid
                GROUP BY e.sourcenodeid
            ) x ON n.nodeid = x.nodeid
        )
        UPDATE nodes n
        SET curr_hub = h.hub_score
        FROM hub h
        WHERE n.nodeid = h.nodeid;

        -- Step 5: 归一化 curr_hub
        WITH total AS (
            SELECT SUM(curr_hub) AS s FROM nodes
        )
        UPDATE nodes
        SET curr_hub = CASE
            WHEN total.s = 0 THEN 0
            ELSE ROUND(curr_hub / total.s, 6)
        END
        FROM total;

        -- Step 6: 判断是否收敛
        SELECT MAX(GREATEST(
            ABS(curr_auth - prev_auth),
            ABS(curr_hub - prev_hub)
        )) INTO diff
        FROM nodes;

        -- 可选调试输出
        -- RAISE NOTICE 'Current diff = %', diff;

        EXIT WHEN diff < margin;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


DO $$
BEGIN
    Perform run_hits();
END
$$;

SELECT * from nodes order by nodeid;