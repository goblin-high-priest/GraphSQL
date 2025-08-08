DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS vertices;

-- 顶点表
CREATE TABLE IF NOT EXISTS vertices(id INT PRIMARY KEY);

-- 无向、带权边表：一条边仅存一行 (u,v,weight)；u≠v
CREATE TABLE IF NOT EXISTS edges(
    u INT REFERENCES vertices(id),
    v INT REFERENCES vertices(id),
    w NUMERIC NOT NULL,
    PRIMARY KEY(u,v)
);
/* ---------- 顶点 ---------- */
INSERT INTO vertices(id) VALUES
  (1),  -- A
  (2),  -- B
  (3),  -- C
  (4),  -- D
  (5);  -- E

/* ---------- 无向边（只存一行，u < v） ---------- */
INSERT INTO edges(u, v, w) VALUES
  (1, 3, 1),   -- A-C
  (4, 5, 2),   -- D-E
  (3, 4, 3),   -- C-D
  (3, 5, 4),   -- C-E
  (2, 3, 5),   -- B-C
  (2, 4, 6),   -- B-D
  (1, 2, 7);   -- A-B
