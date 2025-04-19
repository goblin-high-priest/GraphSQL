-------------------------------------------------------------------
-- 0) 清理 & 初始化
-------------------------------------------------------------------
DROP TABLE IF EXISTS TOutSegs, TInSegs, TE, TVisited CASCADE;
DROP FUNCTION IF EXISTS merge_forward(INT);
DROP FUNCTION IF EXISTS merge_backward(INT);
DROP FUNCTION IF EXISTS combined_path(VARCHAR);

CREATE TABLE TE (
    fid VARCHAR(10),
    tid VARCHAR(10),
    cost INT,
    PRIMARY KEY (fid, tid)
);

-------------------------------------------------------------------
-- 示例：插入数据
-------------------------------------------------------------------
INSERT INTO TE VALUES
    ('s','b',2), ('s','d',6), ('s','c',1),
    ('b','e',2), ('c','d',1), ('c','e',3), 
    ('e','d',8), ('e','f',7), ('e','h',8),
    ('e','g',3), ('f','h',5), ('g','h',4),
    ('h','j',4), ('h','i',1), ('i','t',2), 
    ('j','t',9), ('t','s',7);

select * from TE;

CREATE TABLE TVisited (
    p2s VARCHAR(10),
    d2s INT DEFAULT 999999,
    f INT,
    nid VARCHAR(10),
    b INT,
    d2t INT DEFAULT 999999,
    p2t VARCHAR(10)
);

CREATE TABLE TOutSegs (
    fid  VARCHAR(10),          -- 初识节点 
    tid  VARCHAR(10),
	pid  VARCHAR(10),          -- 前驱
    cost  INT DEFAULT 999999,  -- 到源点 s 的距离
    -- fwd  INT DEFAULT 1,       -- 记录本轮扩展 (i)
    f    INT DEFAULT 0,
	PRIMARY KEY (tid, fid)
);

CREATE TABLE TInSegs (
    fid  VARCHAR(10),          -- 初识节点 
    tid  VARCHAR(10),
	pid  VARCHAR(10),          -- 前驱
    cost  INT DEFAULT 999999,  -- 到源点 s 的距离
    -- bwd  INT DEFAULT 1,       -- 记录本轮扩展 (i)
	PRIMARY KEY (tid, fid),
    b    INT DEFAULT 0
);

-- 初始化起点 s, 终点 t
INSERT INTO TOutSegs (fid, tid, pid, cost) 
SELECT DISTINCT fid, fid, fid, 0
FROM TE;

INSERT INTO TInSegs (fid, tid, pid, cost) 
SELECT DISTINCT tid, tid, tid, 0
FROM TE;

select * from TOutSegs;
select * from TInSegs;

CREATE OR REPLACE FUNCTION build_out_seg(lthd INT, wmin INT)
RETURNS VOID AS $$
DECLARE 
    threshold INT;
    flag INT DEFAULT 0;
    ER_query TEXT;
BEGIN
    FOR i IN 1..CEIL(lthd / wmin) LOOP

        IF i * wmin < lthd THEN
            threshold := i * wmin;
        ELSE
            threshold := lthd;
        END IF;
        
        -- 根据不同的fid进行update 
        UPDATE TOutSegs AS t
        SET f = 2
        FROM (
            SELECT fid, MIN(cost) AS min_cost
            FROM TOutSegs
            WHERE f = 0
            GROUP BY fid
        ) AS tmp
        WHERE (t.cost < threshold OR t.cost = tmp.min_cost)
        AND t.f = 0
        AND t.fid = tmp.fid
        AND tmp.min_cost <= lthd;

        EXECUTE format($ER$
            CREATE OR REPLACE VIEW ER AS
            SELECT 
                TOutSegs.fid            AS fid,  -- 源节点
                TE.tid                  AS tid,  -- 当前节点
                TE.fid                  AS pid,  -- 父节点
                TOutSegs.cost + TE.cost AS cost  -- 到源节点距离 
            FROM TOutSegs, TE
            WHERE TOutSegs.tid = TE.fid 
            AND TOutSegs.cost + TE.cost <= %L
            AND TOutSegs.f = 2
        $ER$, lthd);

        MERGE INTO TOutSegs AS target
        USING (
            SELECT fid, tid, pid, cost
            FROM (
                SELECT
                    er.fid  AS fid,
                    er.tid  AS tid,
                    er.pid  AS pid,
                    er.cost AS cost,
                    ROW_NUMBER() OVER(
                        PARTITION BY fid, tid
                        ORDER BY er.cost
                    ) AS rownum
                FROM er
            ) tmp
            WHERE rownum = 1
        ) AS source(fid, tid, pid, cost)
        ON source.tid = target.tid
        AND source.fid = target.fid
        WHEN MATCHED AND target.cost > source.cost THEN
            UPDATE SET
                cost = source.cost,
                pid = source.pid
        WHEN NOT MATCHED BY target THEN
            INSERT (tid, fid, pid, cost, f) VALUES (source.tid, source.fid, source.pid, source.cost, 0);
        
        UPDATE TOutSegs SET f = 1 WHERE f = 2;

    END LOOP;

    DELETE FROM TOutSegs
    WHERE cost = 0;

    DROP VIEW IF EXISTS ER;
    EXECUTE 'ALTER TABLE TOutSegs DROP COLUMN f';

    MERGE INTO TOutSegs AS target
    USING (
        SELECT fid, fid, tid, cost
        FROM TE
    ) AS source(fid, pid, tid, cost)
    ON source.fid = target.fid
    AND source.tid = target.tid
    WHEN MATCHED AND target.cost > source.cost THEN
        UPDATE SET 
            cost = source.cost,
            pid = source.pid
    WHEN NOT MATCHED BY target THEN
        INSERT (fid, tid, pid, cost) VALUES (source.fid, source.tid, source.pid, source.cost);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION build_in_seg(lthd INT, wmin INT)
RETURNS VOID AS $$
DECLARE 
    threshold INT;
    flag INT DEFAULT 0;
    ER_query TEXT;
BEGIN
    FOR i IN 1..CEIL(lthd / wmin) LOOP

        IF i * wmin < lthd THEN
            threshold := i * wmin;
        ELSE
            threshold := lthd;
        END IF;
        
        -- 根据不同的fid进行update 
        UPDATE TInSegs AS t
        SET b = 2
        FROM (
            SELECT fid, MIN(cost) AS min_cost
            FROM TInSegs
            WHERE b = 0
            GROUP BY fid
        ) AS tmp
        WHERE (t.cost < threshold OR t.cost = tmp.min_cost)
        AND t.b = 0
        AND t.fid = tmp.fid
        AND tmp.min_cost <= lthd;

        EXECUTE format($ER$
            CREATE OR REPLACE VIEW ER AS
            SELECT 
                TInSegs.fid             AS fid,  -- 目标节点
                TE.fid                  AS tid,  -- 源节点
                TE.tid                  AS pid,  -- 父节点
                TInSegs.cost + TE.cost  AS cost  -- 到源节点距离 
            FROM TInSegs, TE
            WHERE TInSegs.tid = TE.tid 
            AND TInSegs.cost + TE.cost <= %L
            AND TInSegs.b = 2
        $ER$, lthd);

        MERGE INTO TInSegs AS target
        USING (
            SELECT fid, tid, pid, cost
            FROM (
                SELECT
                    er.fid  AS fid,
                    er.tid  AS tid,
                    er.pid  AS pid,
                    er.cost AS cost,
                    ROW_NUMBER() OVER(
                        PARTITION BY fid, tid
                        ORDER BY er.cost
                    ) AS rownum
                FROM er
            ) tmp
            WHERE rownum = 1
        ) AS source(fid, tid, pid, cost)
        ON source.tid = target.tid
        AND source.fid = target.fid
        WHEN MATCHED AND target.cost > source.cost THEN
            UPDATE SET
                cost = source.cost,
                pid = source.pid
        WHEN NOT MATCHED BY target THEN
            INSERT (tid, fid, pid, cost, b) VALUES (source.tid, source.fid, source.pid, source.cost, 0);
        
        UPDATE TInSegs SET b = 1 WHERE b = 2;

    END LOOP;

    DELETE FROM TInSegs
    WHERE cost = 0;

    DROP VIEW IF EXISTS ER;
    EXECUTE 'ALTER TABLE TInSegs DROP COLUMN b';

    MERGE INTO TInSegs AS target
    USING (
        SELECT tid, tid, fid, cost
        FROM TE
    ) AS source(fid, pid, tid, cost)
    ON source.fid = target.fid
    AND source.tid = target.tid
    WHEN MATCHED AND target.cost > source.cost THEN
        UPDATE SET 
            cost = source.cost,
            pid = source.pid
    WHEN NOT MATCHED BY target THEN
        INSERT (fid, tid, pid, cost) VALUES (source.fid, source.tid, source.pid, source.cost);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_forward(lb INT, min_cost INT)
RETURNS INT AS $$
DECLARE 
    affected_count INT;
    MAX CONSTANT INT := 999999;
BEGIN
    MERGE INTO TVisited AS target
    USING (
        SELECT nid, p2s, cost
        FROM (
            SELECT 
                out.tid,
                out.pid, 
                out.cost + q.d2s,
                ROW_NUMBER() OVER(
                    PARTITION BY out.tid
                    ORDER BY out.cost + q.d2s
                ) AS rownum
            FROM TVisited q, TOutSegs out
            WHERE q.nid = out.fid
            AND q.f = 2
            AND out.cost + q.d2s + lb < min_cost
        ) tmp(nid, p2s, cost)
        WHERE rownum = 1
    ) AS source(nid, p2s, cost)
    ON source.nid = target.nid
    WHEN MATCHED and target.d2s > source.cost THEN
        UPDATE SET
            d2s = source.cost,
            p2s = source.p2s,
            f = 0
    WHEN NOT MATCHED BY target THEN
        INSERT (nid, d2s, d2t, p2s, f) VALUES (source.nid, cost, MAX, source.p2s, 0);

    GET DIAGNOSTICS affected_count = ROW_COUNT;
    RETURN affected_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_backward(lf INT, min_cost INT)
RETURNS INT AS $$
DECLARE 
    affected_count INT;
    MAX CONSTANT INT := 999999;
BEGIN
    MERGE INTO TVisited AS target
    USING (
        SELECT nid, p2t, cost
        FROM (
            SELECT 
                _in.tid,
                _in.pid, 
                _in.cost + q.d2t,
                ROW_NUMBER() OVER(
                    PARTITION BY _in.tid
                    ORDER BY _in.cost + q.d2t
                ) AS rownum
            FROM TVisited q, TInSegs _in
            WHERE q.nid = _in.fid
            AND q.b = 2
            AND _in.cost + q.d2t + lf < min_cost
        ) tmp(nid, p2t, cost)
        WHERE rownum = 1
    ) AS source(nid, p2t, cost)
    ON source.nid = target.nid
    WHEN MATCHED and target.d2t > source.cost THEN
        UPDATE SET
            d2t = source.cost,
            p2t = source.p2t,
            b = 0
    WHEN NOT MATCHED BY target THEN
        INSERT (nid, d2s, d2t, p2t, b) VALUES (source.nid, MAX, cost, source.p2t, 0);

    GET DIAGNOSTICS affected_count = ROW_COUNT;
    RETURN affected_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION main(lthd INT)
RETURNS VARCHAR(10) AS $$
DECLARE
    MAX CONSTANT INT := 999999;
    min_cost INT := MAX;
    -- min_d2s INT := MAX;
    -- min_d2t INT := MAX;
    lf INT := 0;
    lb INT := 0;
    nf INT := 1;
    nb INT := 1;
    fwd INT := 1;
    bwd INT := 1; 
    xid VARCHAR(10) := '';
BEGIN
    INSERT INTO TVisited (p2s, d2s, f, nid) VALUES ('s', 0, 0, 's');
    INSERT INTO TVisited (nid, b, d2t, p2t) VALUES ('f', 0, 0, 'f');

    WHILE lb + lf <= min_cost AND nf > 0 AND nb > 0 LOOP

        IF nf <= nb THEN
            UPDATE TVisited SET f = 2
            WHERE (d2s <= fwd * lthd
            OR
            d2s = (
                SELECT MIN(d2s)
                FROM TVisited
                WHERE f = 0
            ))
            AND f = 0;

            nf := merge_forward(lb, min_cost);

            UPDATE TVisited SET f = 1 WHERE f = 2;

            SELECT MIN(d2s) INTO lf FROM TVisited WHERE f = 0;

            fwd := fwd + 1;
        ELSE
            UPDATE TVisited SET b = 2
            WHERE (d2t <= bwd * lthd
            OR
            d2t = (
                SELECT MIN(d2t)
                FROM TVisited
                WHERE b = 0
            ))
            AND b = 0;

            nb := merge_backward(lf, min_cost);

            UPDATE TVisited SET b = 1 WHERE b = 2;

            SELECT MIN(d2t) INTO lb FROM TVisited WHERE b = 0;

            bwd := bwd + 1;

        END IF;

        SELECT MIN(d2s+d2t) INTO min_cost FROM TVisited;

    END LOOP;

    SELECT nid INTO xid FROM TVisited WHERE d2s + d2t = min_cost;

    RETURN xid;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION combined_path(xid VARCHAR(10))
RETURNS VOID AS $$
BEGIN
    EXECUTE format($exp$
        CREATE OR REPLACE VIEW path AS
        WITH RECURSIVE
        forward_path(nid, p2s, depth) AS (
            SELECT nid, p2s, 0
            FROM TVisited
            WHERE nid = %L
            UNION ALL
            SELECT fp.p2s, TVisited.p2s, depth + 1
            FROM forward_path fp, TVisited
            WHERE fp.p2s = TVisited.nid
            AND TVisited.p2s <> TVisited.nid
        ),
        backward_path AS (
            SELECT nid, p2t
            FROM TVisited
            WHERE nid = %L
            UNION ALL
            SELECT bp.p2t, TVisited.p2t
            FROM backward_path bp, TVisited
            WHERE bp.p2t = TVisited.nid
            AND TVisited.p2t <> TVisited.nid
        ),
        path AS (
            SELECT *
            FROM (
                SELECT nid, p2s
                FROM forward_path
                ORDER BY depth DESC
            )
            UNION ALL
            SELECT p2t, nid
            FROM backward_path
        )
        SELECT * FROM path;
    $exp$, xid, xid);
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    a INT;
    lthd INT := 6;
    xid VARCHAR(10);
BEGIN
    PERFORM build_out_seg(6, 1);
    PERFORM build_in_seg(6, 1);
    xid = main(lthd);
    -- a := merge_forward(0, 999999);
    -- a := merge_backward(0, 999999);
    -- RAISE NOTICE 'merge_forward function returns %', a;
    RAISE NOTICE 'xid is %', xid;
    PERFORM combined_path(xid);
END;
$$;

select * from TOutSegs order by fid;
select * from TInSegs order by fid;
select * from TVisited;
select * from path;