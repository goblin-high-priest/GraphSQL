DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS vertices;

CREATE TABLE vertices (
    id int PRIMARY KEY
);

CREATE TABLE edges (
    src    int REFERENCES vertices(id),
    dst    int REFERENCES vertices(id),
    weight numeric NOT NULL,
    PRIMARY KEY (src, dst)
);

INSERT INTO vertices (id) VALUES
  (0), (1), (2), (3);

INSERT INTO edges (src, dst, weight) VALUES
  (0, 2,  3),
  (2, 0, -2),
  (0, 1,  4),
  (1, 3,  2),
  (3, 0,  3),
  (2, 3,  5);

TABLE vertices;
TABLE edges;
