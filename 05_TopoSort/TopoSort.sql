-- 第一步：预处理所有点的入度
WITH RECURSIVE in_degree AS (
    SELECT V.ID, COUNT(E.F) AS indeg
    FROM V
    LEFT JOIN E ON V.ID = E.T
    GROUP BY V.ID
),

-- 第二步：递归生成拓扑序
Topo(ID, L, path) AS (
    -- Base case: 所有入度为 0 的点，层级为 0，路径为空
    SELECT ID, 0, ARRAY[ID]
    FROM in_degree
    WHERE indeg = 0

    UNION ALL

    -- Recursive step:
    -- 从当前节点扩展它的所有邻居 T
    SELECT E.T, T.L + 1, path || E.T
    FROM Topo T
    JOIN E ON E.F = T.ID

    -- 只允许那些 T 的所有前驱都在 path 中（即都已拓扑排序）
    WHERE NOT EXISTS (
        SELECT 1
        FROM E E2
        WHERE E2.T = E.T AND E2.F <> ALL(T.path)
    )
)

SELECT ID, L FROM Topo
ORDER BY L;
