DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS vertices;
DROP TABLE IF EXISTS vertex_map;

CREATE TABLE vertex_map (
  name TEXT PRIMARY KEY,
  id   INT  UNIQUE
);

INSERT INTO vertex_map(name,id) VALUES
  ('A',1), ('B',2), ('C',3),
  ('D',4), ('E',5), ('F',6);


CREATE TABLE vertices (
  id INT PRIMARY KEY
);

INSERT INTO vertices(id)
SELECT id FROM vertex_map;


CREATE TABLE edges (
  source  INT REFERENCES vertices(id),
  target  INT REFERENCES vertices(id),
  weight  INT,
  PRIMARY KEY (source,target)
);

CREATE INDEX edges_source_idx ON edges(source);

INSERT INTO edges(source,target,weight) VALUES
  (1,2, 6),
  (1,3, 4),
  (1,4, 5),
  (2,5,-1),
  (3,2,-2),
  (3,5, 3),
  (4,3,-2),
  (4,6,-1),
  (5,6, 3);
