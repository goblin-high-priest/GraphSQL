-------------------------------------------------------------------
-- 0) 清理 & 初始化
-------------------------------------------------------------------
DROP TABLE IF EXISTS TAf, TAb, TE CASCADE;
DROP FUNCTION IF EXISTS merge_forward(INT);
DROP FUNCTION IF EXISTS merge_backward(INT);
-------------------------------------------------------------------
-- 1) 创建 TAf, TAb
-------------------------------------------------------------------
CREATE TABLE TAf (
    nid  VARCHAR(10) PRIMARY KEY,
	p2s  VARCHAR(10),         -- 前驱
    d2s  INT DEFAULT 999999,  -- 到源点 s 的距离
    fwd  INT DEFAULT 0        -- 记录本轮扩展 (i)
);

CREATE TABLE TAb (
    nid  VARCHAR(10) PRIMARY KEY,
    p2s  VARCHAR(10),
    d2t  INT DEFAULT 999999,  -- 到终点 t 的距离
    bwd  INT DEFAULT 0        -- 记录本轮扩展 (j)
);

-- 初始化起点 s, 终点 t
INSERT INTO TAf (nid, p2s, d2s, fwd) VALUES ('s', 's', 0, 1);
INSERT INTO TAb (nid, p2s, d2t, bwd) VALUES ('h', 'h', 0, 1);

CREATE TABLE TE (
    fid VARCHAR(10),
    tid VARCHAR(10),
    cost INT,
    PRIMARY KEY (fid, tid, cost)
) PARTITION BY RANGE(cost);

CREATE TABLE TE_1  PARTITION OF TE FOR VALUES FROM (1) TO (10);
CREATE TABLE TE_2  PARTITION OF TE FOR VALUES FROM (11) TO (20);
CREATE TABLE TE_3  PARTITION OF TE FOR VALUES FROM (21) TO (999999);

-------------------------------------------------------------------
-- 示例：插入数据
-------------------------------------------------------------------
INSERT INTO TE VALUES
    ('s','b',6), ('s','d',6), ('s','t',22),
    ('b','c',9), ('b','g',12), ('c','s',1),
    ('c','d',12), ('c','e',32), ('d','e',8),
    ('e','b',2), ('e','f',27), ('e','h',3),
    ('e','g',16), ('f','h',5), ('g','h',4),
    ('h','j',4), ('h','t',9), ('i','h',1),
    ('i','t',2), ('j','t',9);

-- select * from TE_3;

CREATE OR REPLACE FUNCTION build_forward_er_view(
    l INT,
    i INT,
    min_cost INT,
    lbj INT,
	pts INT
) RETURNS VOID AS $$
DECLARE
    ER_query TEXT := '';
    union_piece TEXT;
    k INT;
BEGIN
    -- FOR k IN l..LEAST(i, pts) LOOP
	FOR k IN l..i LOOP
        union_piece := format(
            'SELECT TE.tid, TE.fid, TF.d2s + TE.cost AS d2s
             FROM TAF TF
             JOIN %s TE ON TF.fwd = %L AND TF.nid = TE.fid
             WHERE TF.d2s + TE.cost < %s',
            'te_' || i-k+1, k, min_cost - lbj
        );

        IF ER_query = '' THEN
            ER_query := union_piece;
        ELSE
            ER_query := ER_query || E'\n' || 'UNION ALL' || E'\n' || union_piece;
        END IF;
    END LOOP;

    ER_query := 'CREATE OR REPLACE VIEW ER(id, p2s, d2s) AS' || E'\n' || ER_query;

    -- RAISE NOTICE 'Final SQL:\n%s', ER_query;
	EXECUTE 'DROP VIEW IF EXISTS ER';
    EXECUTE ER_query;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION build_backward_er_view(
    l INT,
    j INT,
    min_cost INT,
    lfi INT,
	pts INT
) RETURNS VOID AS $$
DECLARE
    ER_query TEXT := '';
    union_piece TEXT;
    k INT;
BEGIN
    -- FOR k IN l..LEAST(j, pts) LOOP
	FOR k IN l..j LOOP
        union_piece := format(
            'SELECT TE.fid, TE.tid, TB.d2t + TE.cost AS d2t
             FROM TAB TB
             JOIN %s TE ON TB.bwd = %L AND TB.nid = TE.tid
             WHERE TB.d2t + TE.cost < %s',
            'te_' || j-k+1, k, min_cost - lfi
        );

        IF ER_query = '' THEN
            ER_query := union_piece;
        ELSE
            ER_query := ER_query || E'\n' || 'UNION ALL' || E'\n' || union_piece;
        END IF;
    END LOOP;

    ER_query := 'CREATE OR REPLACE VIEW ER(id, p2s, d2t) AS' || E'\n' || ER_query;

    -- RAISE NOTICE 'Final SQL:\n%s', ER_query;
	-- IF ER_query <> 'CREATE OR REPLACE VIEW ER(id, p2s, d2t) AS' THEN
 --    	EXECUTE 'DROP VIEW IF EXISTS ER';
 --    	EXECUTE ER_query;
	-- ELSE
 --    	RAISE NOTICE 'No valid backward expansion at level %', j;
	-- END IF;
	EXECUTE 'DROP VIEW IF EXISTS ER';
    EXECUTE ER_query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_forward(i INT)
RETURNS INT AS $$
DECLARE 
	affected_count INT;
BEGIN
	MERGE INTO TAf AS target
	USING (
		SELECT nid, p2s, d2s AS cost
		FROM (
			SELECT 
				er.id AS nid, 
				er.p2s AS p2s, 
				er.d2s AS d2s,
				ROW_NUMBER() OVER(
					PARTITION BY er.id
					ORDER BY er.d2s
				) AS rownum
			FROM er
		) tmp
		WHERE rownum = 1
	) AS source(nid, p2s, cost)
	ON source.nid = target.nid
	WHEN MATCHED AND target.d2s > source.cost THEN
		UPDATE SET 
			d2s = source.cost,
			p2s = source.p2s,
			fwd = i + 1
	WHEN NOT MATCHED BY target THEN
		INSERT (nid, d2s, p2s, fwd) VALUES (source.nid, cost, source.p2s, i+1);
	
	GET DIAGNOSTICS affected_count = ROW_COUNT;
	RETURN affected_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_backward(j INT)
RETURNS INT AS $$
DECLARE 
	affected_count INT;
BEGIN
	MERGE INTO TAb AS target
	USING (
		SELECT nid, p2s, d2t AS cost
		FROM (
			SELECT 
				er.id AS nid, 
				er.p2s AS p2s, 
				er.d2t AS d2t,
				ROW_NUMBER() OVER(
					PARTITION BY er.id
					ORDER BY er.d2t
				) AS rownum
			FROM er
		) tmp
		WHERE rownum = 1
	) AS source(nid, p2s, cost)
	ON source.nid = target.nid
	WHEN MATCHED AND target.d2t > source.cost THEN
		UPDATE SET 
			d2t = source.cost,
			p2s = source.p2s,
			bwd = j + 1
	WHEN NOT MATCHED BY target THEN
		INSERT (nid, d2t, p2s, bwd) VALUES (source.nid, cost, source.p2s, j+1);
	
	GET DIAGNOSTICS affected_count = ROW_COUNT;
	RETURN affected_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION compute_lfi(i INT, old_lfi INT)
RETURNS INT AS $$
DECLARE 
	min_d2s INT;
	new_lfi INT;
BEGIN
	SELECT MIN(d2s) INTO min_d2s FROM TAf WHERE fwd = i + 1;
	IF i = 1 THEN
		new_lfi := LEAST(min_d2s, 10);
	ELSE
		new_lfi := LEAST(old_lfi + 10, min_d2s);
	END IF;
	RETURN new_lfi;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION compute_lbj(j INT, old_lbj INT)
RETURNS INT AS $$
DECLARE 
	min_d2t INT;
	new_lbj INT;
BEGIN
	SELECT MIN(d2t) INTO min_d2t FROM TAb WHERE bwd = j + 1;
	IF j = 1 THEN
		new_lbj := LEAST(min_d2t, 10);
	ELSE
		new_lbj := LEAST(old_lbj + 10, min_d2t);
	END IF;
	RETURN new_lbj;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION main()
RETURNS VARCHAR(10) AS $$
DECLARE
	min_cost INT := 999999;
	i INT := 1; 
	j INT := 1;
	lfi INT := 0;
	lbj INT := 0;
	nf INT := 1;
	nb INT := 1;
	pts INT := 3;
	ER_query TEXT := '';
	union_piece TEXT;
	l INT := 1;  
	k INT;
	-- affected_count INT;
	min_d2s INT;
	tmp_cost INT;
	verified_cost INT;
	xid VARCHAR(10);
	merge_times INT := 0;
BEGIN
	WHILE lfi + lbj <= min_cost LOOP
	
		IF nf <= nb THEN
		-- IF nf > nb THEN
			l := GREATEST(1, i - pts + 1);
			PERFORM build_forward_er_view(l, i, min_cost, lbj, pts);
			nf := merge_forward(i);
			RAISE NOTICE 'nf is %', nf;

			IF nf <> 0 THEN
				merge_times := merge_times + 1;
			END IF;
			-- EXIT WHEN nf = 0;
			lfi := compute_lfi(i, lfi);
			RAISE NOTICE 'Minimal d2s for fwd = % is %', i + 1, lfi;
			i := i + 1;
	
		ELSE
			l := GREATEST(1, j - pts + 1);
			PERFORM build_backward_er_view(l, j, min_cost, lfi, pts);
			nb := merge_backward(j);
			RAISE NOTICE 'nb is %', nb;
			IF nb <> 0 THEN
				merge_times := merge_times + 1;
			END IF;
			-- EXIT WHEN nb = 0;
			lbj := compute_lbj(j, lbj);
			RAISE NOTICE 'Minimal d2t for bwd = % is %', j + 1, lbj;
			j := j + 1;
			
		END IF;

		SELECT COALESCE(MIN(d2s + d2t), 99999) INTO tmp_cost FROM TAf TF, TAb TB, TE WHERE TF.nid = TB.nid;
		min_cost := tmp_cost;
		RAISE NOTICE 'min_cost is %', min_cost;
	END LOOP;

	RAISE NOTICE 'merge_times is %', merge_times;

	SELECT MIN(d2s + cost + d2t) INTO verified_cost FROM TAf TF, TAb TB, TE WHERE TF.nid = TE.fid AND TE.tid = TB.nid;
	verified_cost := LEAST(verified_cost, min_cost);
	RAISE NOTICE 'verified_cost is %', verified_cost;

	SELECT TF.nid INTO xid FROM TAf TF, TAb TB WHERE TF.nid = TB.nid AND d2s + d2t = verified_cost;
	RAISE NOTICE 'xid is %', xid;
	RETURN xid;
	-- PERFORM compute_path(xid);
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM ER;
-- SELECT * FROM TAf;
-- SELECT * FROM combined_path;

-- SELECT main();

-- PERFORM * FROM combined_path;
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

SELECT * FROM path;
select * from TAf;
select * from TAb;
