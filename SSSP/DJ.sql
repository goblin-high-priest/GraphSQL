-- 1. 初始化 TA 表
-- DROP VIEW IF EXISTS ER;
-- DROP TABLE IF EXISTS TA;
------------------------------------------------------------
-- 1) 清理现场，创建图的节点表与边表，并插入示例数据
------------------------------------------------------------
DROP TABLE IF EXISTS TE, TN CASCADE;

CREATE TABLE TN (
    nid VARCHAR(10) PRIMARY KEY
);

CREATE TABLE TE (
    fid VARCHAR(10),  -- from-node id
    tid VARCHAR(10),  -- to-node id
    cost INT,
    PRIMARY KEY (fid, tid),
    FOREIGN KEY (fid) REFERENCES TN(nid),
    FOREIGN KEY (tid) REFERENCES TN(nid)
);

------------------------------------------------------------
-- 2) 创建 TA 表，并插入起点 's'
------------------------------------------------------------
DROP TABLE IF EXISTS TA;

CREATE TABLE TA (
    nid VARCHAR(10) PRIMARY KEY,  -- 节点 ID
    d2s INT DEFAULT 999999,       -- 源点到该节点的最短距离
    p2s VARCHAR(10),              -- 前驱节点
    ff INT DEFAULT 0              -- 0=未确定, 1=已确定
);

-- 初始化：插入起点 s（距离=0, 前驱=s, ff=0）
INSERT INTO TA (nid, d2s, p2s, ff) VALUES ('s', 0, 's', 0);

-- 示例：插入节点
INSERT INTO TN (nid) VALUES 
    ('s'), ('b'), ('c'), ('d'), ('e'), 
    ('f'), ('g'), ('h'), ('j'), ('t'), ('i');

-- 示例：插入若干带权边
INSERT INTO TE (fid, tid, cost) VALUES 
    ('s', 'b', 6),
    ('s', 'd', 6),
    ('s', 't', 22),
    ('b', 'c', 9),
    ('b', 'g', 12),
    ('c', 's', 1),
    ('c', 'd', 12),
    ('c', 'e', 32),
    ('d', 'e', 8),
    ('e', 'b', 2),
    ('e', 'f', 27),
    ('e', 'h', 3),
    ('e', 'g', 16),
    ('f', 'h', 5),
    ('g', 'h', 4),
    ('h', 'j', 4),
    ('h', 't', 9),
    ('i', 'h', 1),
    ('i', 't', 2),
    ('j', 't', 9);
------------------------------------------------------------
-- 3) 以单向 Dijkstra 方式，循环取距离最小的 ff=0 节点并扩展
------------------------------------------------------------
DO $$
DECLARE
    mid  VARCHAR(10);
	affected_count INT := 0 ;
	merge_times INT := 0;
BEGIN
    LOOP
        -- (a) 从 TA 中选取尚未确定(ff=0)且距离 d2s 最小的节点
        SELECT nid INTO mid
        FROM TA
        WHERE ff = 0
        ORDER BY d2s
        LIMIT 1;

        -- SELECT nid
        -- INTO mid
        -- FROM TAf
        -- WHERE ff = 0
        -- AND d2s = (
        --     SELECT MIN(d2s)
        --     FROM TAf
        --     WHERE ff = 0
        -- );


        -- 若没有任何 ff=0 节点，则说明搜索完成，退出循环
		EXIT WHEN NOT FOUND;

        -- 若该节点就是目标 t，则退出循环
		EXIT WHEN mid = 'f';
		
        EXECUTE format($exp$
            CREATE OR REPLACE VIEW ER AS
            SELECT 
                TE.tid           AS nid,        -- 邻居节点 ID
                TE.fid           AS p2s,        -- 前驱是当前节点
                TE.cost + TA.d2s AS cost       -- 计算候选距离
            FROM TA 
            JOIN TE ON TA.nid = TE.fid
            WHERE TA.nid = %L
            -- AND TA.ff = 0
        $exp$, mid);

        -- (d) 用 MERGE 语句把这些候选更新 合并到 TA 里
        MERGE INTO TA AS target
        USING (
            SELECT nid, p2s, cost
            FROM (
                SELECT 
                    nid, p2s, cost,
                    ROW_NUMBER() OVER (
                        PARTITION BY nid
                        ORDER BY cost
                    ) AS rownum
                FROM ER
            )
            WHERE rownum = 1
        ) AS source
        ON (source.nid = target.nid)
        WHEN MATCHED AND target.d2s > source.cost THEN
            UPDATE SET 
                d2s = source.cost,
                p2s = source.p2s
        WHEN NOT MATCHED THEN
            INSERT (nid, d2s, p2s, ff)
            VALUES (source.nid, source.cost, source.p2s, 0);

		merge_times := merge_times + 1;
		RAISE NOTICE 'merge_times is %', merge_times;
		GET DIAGNOSTICS affected_count = ROW_COUNT;
		EXIT WHEN affected_count = 0;

        UPDATE TA
        SET ff = 1
        WHERE nid = mid;

    END LOOP;
END;
$$;

------------------------------------------------------------
-- 4) 反向回溯：从 't' 开始，递归找到最终的路径(若存在)
------------------------------------------------------------
WITH RECURSIVE path AS (
    -- 起点：目标节点 t
    SELECT nid, p2s
      FROM TA
     WHERE nid = 'h'

    UNION ALL

    -- 递归：向前驱节点回溯
    -- SELECT TA.nid, TA.p2s
	SELECT path.p2s, TA.p2s
      FROM TA
      JOIN path ON TA.nid = path.p2s
	  WHERE TA.p2s <> TA.nid -- 避免死循环
)

SELECT * FROM path;

-- select * from er;
-- SELECT * FROM TA;
