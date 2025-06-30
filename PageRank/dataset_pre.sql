DROP TABLE IF EXISTS Nodes;
CREATE TABLE Nodes (
    NodeId int not null,
    NodeWeight decimal(10, 5) not null,
    NodeCount int not null default(0),
    HasConverged boolean not null default false,
    constraint NodesPK primary key (NodeId)
);

CLUSTER Nodes USING NodesPK;

DROP TABLE IF EXISTS Edges;
CREATE TABLE Edges (
    SourceNodeId int not null,
    TargetNodeId int not null,
    constraint EdgesPK primary key (SourceNodeId, TargetNodeId),
    constraint EdgeChk check (SourceNodeId <> TargetNodeId) --ignore self references
);

CLUSTER Edges USING edgespk;

INSERT INTO Nodes (NodeId, NodeWeight) VALUES
    (1, 0.25),
    (2, 0.25),
    (3, 0.25),
    (4, 0.25);

INSERT INTO Edges (SourceNodeId, TargetNodeId) VALUES
    (2, 1),
    (2, 3),
    (3, 1),
    (4, 1),
    (4, 2),
    (4, 3);

SELECT * FROM nodes;
SELECT * FROM edges;