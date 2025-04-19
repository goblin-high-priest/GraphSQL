-------------------------------------------------------------------
-- 0) 清理 & 初始化
-------------------------------------------------------------------
DROP TABLE IF EXISTS TAf, TAb, TE CASCADE;
DROP FUNCTION IF EXISTS merge_forward();
DROP FUNCTION IF EXISTS merge_backward();
DROP FUNCTION IF EXISTS build_forward_er_view();
DROP FUNCTION IF EXISTS build_backward_er_view();
-------------------------------------------------------------------
-- 1) 创建 TAf, TAb
-------------------------------------------------------------------
CREATE TABLE TAf (
    nid  VARCHAR(10) PRIMARY KEY,
	p2s  VARCHAR(10),         -- 前驱
    d2s  INT DEFAULT 999999,  -- 到源点 s 的距离
    f  INT DEFAULT 0       
);

CREATE TABLE TAb (
    nid  VARCHAR(10) PRIMARY KEY,
    p2s  VARCHAR(10),
    d2t  INT DEFAULT 999999,  -- 到终点 t 的距离
    b  INT DEFAULT 0        
);

-- 初始化起点 s, 终点 t
INSERT INTO TAf (nid, p2s, d2s, f) VALUES ('s', 's', 0, 0);
INSERT INTO TAb (nid, p2s, d2t, b) VALUES ('f', 'f', 0, 0);

CREATE TABLE TE (
    fid VARCHAR(10),
    tid VARCHAR(10),
    cost INT,
    PRIMARY KEY (fid, tid, cost)
);

INSERT INTO TE VALUES
    ('s','b',6), ('s','d',6), ('s','t',22),
    ('b','c',9), ('b','g',12), ('c','s',1),
    ('c','d',12), ('c','e',32), ('d','e',8),
    ('e','b',2), ('e','f',27), ('e','h',3),
    ('e','g',16), ('f','h',5), ('g','h',4),
    ('h','j',4), ('h','t',9), ('i','h',1),
    ('i','t',2), ('j','t',9);

CREATE OR REPLACE FUNCTION build_forward_er_view()
RETURNS VARCHAR AS $$
DECLARE
    mid VARCHAR(10);
BEGIN
    -- UPDATE TAf
    -- SET f = 2
    -- WHERE TAf.d2s = (
    --     SELECT MIN(d2s) FROM TAf 
    --     WHERE TAf.f = 0
    -- );
    SELECT nid INTO mid
    FROM TAf
    WHERE f = 0
    ORDER BY d2s
    LIMIT 1;

    EXECUTE format($forward_er$
    CREATE OR REPLACE VIEW er AS
    SELECT
        TE.tid            AS nid,
        TE.fid            AS p2s,
        TAf.d2s + TE.cost AS cost
    FROM TE, TAf
    WHERE TE.fid = TAf.nid
    AND TAf.nid = %L
    $forward_er$, mid);
    RETURN mid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_forward(mid VARCHAR)
RETURNS INT AS $$
DECLARE
    affected_count INT;
BEGIN
    MERGE INTO TAf AS target
    USING (
        SELECT nid, p2s, cost
        FROM (
            SELECT 
            nid, p2s, cost,
            ROW_NUMBER() OVER(
                PARTITION BY nid
                ORDER BY cost
            ) AS rownum
            FROM er
        )
        WHERE rownum = 1
    ) AS source
    ON (source.nid = target.nid)
    WHEN MATCHED AND target.d2s > source.cost THEN
        UPDATE SET
            d2s = source.cost,
            p2s = source.p2s
    WHEN NOT MATCHED THEN
        INSERT (nid, p2s, d2s) VALUES (source.nid, source.p2s, source.cost);

    GET DIAGNOSTICS affected_count = ROW_COUNT;
    RAISE NOTICE 'affected_count is %', affected_count;

    -- UPDATE TAf
    -- SET f = 1
    -- WHERE TAf.f = 2;
    UPDATE TAf
    SET f = 1
    WHERE TAf.nid = mid;

    RETURN affected_count;    
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION build_backward_er_view()
RETURNS VARCHAR AS $$
DECLARE
    mid VARCHAR(10);
BEGIN
    -- UPDATE TAb
    -- SET b = 2
    -- WHERE TAb.d2t = (
    --     SELECT MIN(d2t) FROM TAb 
    --     WHERE TAb.b = 0
    -- );
    SELECT nid INTO mid
    FROM TAb
    WHERE b = 0
    ORDER BY d2t
    LIMIT 1;

    CREATE OR REPLACE VIEW er AS
    SELECT
        TE.fid            AS nid,
        TE.tid            AS p2s,
        TAb.d2t + TE.cost AS cost
    FROM TE, TAb
    WHERE TE.tid = TAb.nid
    AND TAb.b = 2;

    RETURN mid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_backward(mid VARCHAR)
RETURNS INT AS $$
DECLARE
    affected_count INT;
BEGIN
    MERGE INTO TAb AS target
    USING (
        SELECT nid, p2s, cost
        FROM (
            SELECT 
            nid, p2s, cost,
            ROW_NUMBER() OVER(
                PARTITION BY nid
                ORDER BY cost
            ) AS rownum
            FROM er
        )
        WHERE rownum = 1
    ) AS source
    ON (source.nid = target.nid)
    WHEN MATCHED AND target.d2t > source.cost THEN
        UPDATE SET
            d2t = source.cost,
            p2s = source.p2s
    WHEN NOT MATCHED THEN
        INSERT (nid, p2s, d2t) VALUES (source.nid, source.p2s, source.cost);

    GET DIAGNOSTICS affected_count = ROW_COUNT;
    RAISE NOTICE 'affected_count is %', affected_count;

    -- UPDATE TAb
    -- SET b = 1
    -- WHERE TAb.b = 2;
    UPDATE TAb
    SET b = 1
    WHERE TAb.nid = mid;

    RETURN affected_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION main()
RETURNS VARCHAR(10) AS $$
DECLARE
    MAX CONSTANT INT := 999999;
    tmp_cost INT;
    min_cost INT := MAX;
    min_d2s INT := MAX;
    min_d2t INT := MAX;
    lf INT := 0;
    lb INT := 0;
    nf INT := 1;
    nb INT := 1;
    fwd INT := 1;
    bwd INT := 1; 
    mid VARCHAR(10);
    xid VARCHAR(10) := '';
BEGIN
    WHILE lb + lf < min_cost AND nf > 0 AND nb > 0 LOOP

        IF nf <= nb THEN
            mid := build_forward_er_view();
            
            nf := merge_forward(mid);

            IF nf = 0 THEN
                nf := MAX;
            END IF;

            RAISE NOTICE 'nf is %', nf;
            SELECT COALESCE(MIN(d2s), 0) INTO lf FROM TAf WHERE f = 0;
            RAISE NOTICE 'lf is %', lf;
        ELSE
            mid := build_backward_er_view();
            
            nb := merge_backward(mid);

            IF nb = 0 THEN
                nb := MAX;
            END IF;

            RAISE NOTICE 'nb is %', nb;
            SELECT COALESCE(MIN(d2t), 0) INTO lb FROM TAb WHERE b = 0;
            RAISE NOTICE 'lb is %', lb;
        END IF;

        SELECT COALESCE(MIN(d2s + d2t), MAX) INTO tmp_cost FROM TAf TF, TAb TB, TE WHERE TF.nid = TB.nid;
		min_cost := tmp_cost;
        RAISE NOTICE 'min_cost is %', min_cost;
    END LOOP;

    SELECT TF.nid INTO xid FROM TAf TF, TAb TB WHERE TF.nid = TB.nid AND d2s + d2t = min_cost;
    RAISE NOTICE 'xid is %', xid;
    RETURN xid;

    -- PERFORM build_forward_er_view();
    -- affected_count :=  merge_forward();
    -- PERFORM build_backward_er_view();
    -- affected_count := merge_backward();
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
			FROM TAf
			WHERE nid = %L
			UNION ALL
			SELECT fp.p2s, TAf.p2s, depth + 1
			FROM forward_path fp, TAf
			WHERE fp.p2s = TAf.nid
			AND TAf.p2s <> TAf.nid
		),
		backward_path AS (
			SELECT nid, p2s
			FROM TAb
			WHERE nid = %L
			UNION ALL
			SELECT bp.p2s, TAb.p2s
			FROM backward_path bp, TAb
			WHERE bp.p2s = TAb.nid
			AND TAb.p2s <> TAb.nid
		),
		path AS (
			SELECT * 
			FROM (
				SELECT nid, p2s
				FROM forward_path
				ORDER BY depth DESC
			)
			UNION ALL
			SELECT p2s, nid
			FROM backward_path
		)
		SELECT * FROM path;
	$exp$, xid, xid);
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
	xid VARCHAR(10);
BEGIN
	xid := main();
	PERFORM combined_path(xid);
	-- SELECT * FROM path;
END;
$$;

select * from path;
-- select * from er;
select * from TAf;
select * from TAb;

