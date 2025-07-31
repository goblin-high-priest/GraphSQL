/* ---------- 1. 顶点映射（可选，但推荐） ---------- */
DROP TABLE IF EXISTS vertex_map;
CREATE TABLE vertex_map (
  name TEXT PRIMARY KEY,
  id   INT  UNIQUE
);

INSERT INTO vertex_map(name,id) VALUES
  ('A',1), ('B',2), ('C',3),
  ('D',4), ('E',5), ('F',6);

/* ---------- 2. 顶点表 ---------- */
DROP TABLE IF EXISTS vertices;
CREATE TABLE vertices (
  id INT PRIMARY KEY
);

INSERT INTO vertices(id)
SELECT id FROM vertex_map;

/* ---------- 3. 边表 ---------- */
DROP TABLE IF EXISTS edges;
CREATE TABLE edges (
  source  INT REFERENCES vertices(id),
  target  INT REFERENCES vertices(id),
  weight  INT,
  PRIMARY KEY (source,target)
);

/* 常用走向查询时，给 source 建 B‑tree 索引 */
CREATE INDEX edges_source_idx ON edges(source);

/* ---------- 4. 写入图中 9 条有向边 ---------- */
INSERT INTO edges(source,target,weight) VALUES
  -- A →
  (1,2,  6),   -- A→B 6
  (1,3,  4),   -- A→C 4
  (1,4,  5),   -- A→D 5
  -- B →
  (2,5, -1),   -- B→E -1
  -- C →
  (3,2, -2),   -- C→B -2
  (3,5,  3),   -- C→E 3
  -- D →
  (4,3, -2),   -- D→C -2
  (4,6, -1),   -- D→F -1
  -- E →
  (5,6,  3);   -- E→F 3
