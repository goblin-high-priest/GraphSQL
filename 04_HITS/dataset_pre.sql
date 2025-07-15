DROP TABLE IF EXISTS Nodes;
CREATE TABLE Nodes (
    NodeId INT NOT NULL,
    prev_auth DECIMAL(5, 3) NOT NULL DEFAULT 1,
    prev_hub DECIMAL(5, 3) NOT NULL DEFAULT 1,
    curr_auth DECIMAL(5, 3) NOT NULL DEFAULT 1,
    curr_hub DECIMAL(5, 3) NOT NULL DEFAULT 1,
--     HasConverged BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT NodesPK PRIMARY KEY (NodeId)
);

CLUSTER Nodes USING NodesPK;

DROP TABLE IF EXISTS Edges;
CREATE TABLE Edges (
    SourceNodeId INT NOT NULL,
    TargetNodeId INT NOT NULL,
    CONSTRAINT EdgesPK PRIMARY KEY (SourceNodeId, TargetNodeId)
);

CLUSTER Edges USING EdgesPK;

INSERT INTO Nodes (NodeId) VALUES
    (1),
    (2),
    (3),
    (4);

INSERT INTO Edges (SourceNodeId, TargetNodeId) VALUES
    (1, 3),
    (2, 1),
    (3, 1),
    (3, 2),
    (4, 1),
    (4, 2),
    (4, 3),
    (4, 4);

SELECT * FROM Nodes;

SELECT * FROM Edges;