-- ⚙️ 如已存在同名表先删除
DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS vertices;

-- 1️⃣ 顶点表（共 4 个顶点 0-3）
CREATE TABLE vertices (
    id int PRIMARY KEY
);

-- 2️⃣ 边表（有向＋带权）
CREATE TABLE edges (
    src    int REFERENCES vertices(id),
    dst    int REFERENCES vertices(id),
    weight numeric NOT NULL,
    PRIMARY KEY (src, dst)
);

-- 3️⃣ 插入顶点
INSERT INTO vertices (id) VALUES
  (0), (1), (2), (3);

-- 4️⃣ 插入边（src → dst, weight）
-- 对应图片中的 6 条箭头及权值
INSERT INTO edges (src, dst, weight) VALUES
  (0, 2,  3),   -- 0 → 2  (3)
  (2, 0, -2),   -- 2 → 0  (-2)
  (0, 1,  4),   -- 0 → 1  (4)
  (1, 3,  2),   -- 1 → 3  (2)
  (3, 0,  3),   -- 0 → 3  (3)
  (2, 3,  5);   -- 2 → 3  (5)

-- ✅ 检查一下
TABLE vertices;
TABLE edges;
