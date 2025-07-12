DROP TABLE IF EXISTS E;
DROP TABLE IF EXISTS V;
-- 顶点表（Vertices）
CREATE TABLE V (
    ID INTEGER PRIMARY KEY
);

-- 边表（Edges），表示从 F 指向 T 的有向边
CREATE TABLE E (
    F INTEGER,  -- from node
    T INTEGER,  -- to node
    FOREIGN KEY (F) REFERENCES V(ID),
    FOREIGN KEY (T) REFERENCES V(ID)
);

INSERT INTO V (ID) VALUES
    (1),
    (2),
    (3),
    (4);

INSERT INTO E (F, T) VALUES
    (1, 2),
    (1, 4),
    (2, 3),
    (2, 4),
    (4, 3);

SELECT * FROM V;
SELECT * FROM E;