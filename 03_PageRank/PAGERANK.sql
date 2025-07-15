CREATE OR REPLACE FUNCTION out_degree_calculation()
RETURNS VOID AS $$

DECLARE
    TotalNodeCount INT := (SELECT COUNT(*) FROM Nodes);
BEGIN
    WITH CTE AS (
        SELECT n.nodeid, COALESCE(x.out_deg, TotalNodeCount) AS out_deg
        FROM nodes n
        LEFT OUTER JOIN (
            SELECT e.sourcenodeid, COUNT(*) AS out_deg
            FROM edges e
            GROUP BY e.sourcenodeid
        ) x ON x.sourcenodeid = n.nodeid
        ORDER BY n.nodeid
    )
    UPDATE nodes AS n
    SET nodecount = CTE.out_deg
    FROM CTE
    WHERE n.nodeid = CTE.nodeid;
END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION iteration()
RETURNS VOID AS $$
DECLARE
    damping_factor decimal(3,2) := 0.85;
    margin_of_error decimal(10,5) := 0.001;
    iteration_count INT := 0;
    TotalNodeCount INT := (SELECT COUNT(*) FROM Nodes);
BEGIN
    LOOP
        WITH new_weights AS (
            SELECT
                n.nodeid,
                n.nodeweight AS old_weight,
                1.0 - damping_factor + COALESCE(x.transferweight, 0.0) AS new_weight
            FROM nodes n
            LEFT JOIN (
                SELECT
                    e.targetnodeid,
                    SUM(n.nodeweight / n.nodecount) * damping_factor AS transferweight
                FROM nodes n
                JOIN edges e ON n.nodeid = e.sourcenodeid
                GROUP BY e.targetnodeid
            ) x ON x.targetnodeid = n.nodeid
        )
        UPDATE nodes n
        SET
            nodeweight = nw.new_weight,
            hasconverged = CASE
                WHEN ABS(nw.old_weight - nw.new_weight) < margin_of_error THEN TRUE
                ELSE FALSE
            END
        FROM new_weights nw
        WHERE nw.nodeid = n.nodeid;

        RAISE NOTICE 'Iteration %', iteration_count;
        -- 可以选择将结果写入临时表或 log 表用于调试

        iteration_count := iteration_count + 1;

        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM Nodes WHERE HasConverged = FALSE
        );
    END LOOP;
END;

$$ LANGUAGE plpgsql;

DO $$
DECLARE
    TotalNodeCount INT := (SELECT COUNT(*) FROM Nodes);
BEGIN
    RAISE NOTICE 'Total node count: %', TotalNodeCount;
    PERFORM out_degree_calculation();
    PERFORM iteration();

END
$$;

SELECT * FROM nodes;
