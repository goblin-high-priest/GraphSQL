select * from G;

DROP TABLE IF EXISTS E;
CREATE TABLE E AS
SELECT v, w FROM G
UNION ALL
SELECT w, v FROM G;  -- 无向图对称边

select * from E;

DROP FUNCTION IF EXISTS contract_graph;
CREATE FUNCTION contract_graph() RETURNS void AS $$
DECLARE
    i INT := 0;
    rowcount INT;
    A INT;
    B INT;
BEGIN
    DROP TABLE IF EXISTS S;
    CREATE TEMP TABLE S (id INT, a INT, b INT);

    LOOP
        i := i + 1;

        -- 随机选择两个点作为哈希函数参数
        SELECT v INTO A FROM E ORDER BY random() LIMIT 1;
        SELECT w INTO B FROM E ORDER BY random() LIMIT 1;

        INSERT INTO S VALUES (i, A, B);

        EXECUTE format('
            DROP TABLE IF EXISTS Ri%s;
            CREATE TABLE Ri%s AS
            SELECT v, LEAST(axb(%s, v, %s), MIN(axb(%s, w, %s))) AS r
            FROM E GROUP BY v;
        ', i, i, A, B, A, B);

        EXECUTE format('
            DROP TABLE IF EXISTS T;
            CREATE TABLE T AS
            SELECT DISTINCT V.r AS v, W.r AS w
            FROM E
            JOIN Ri%s V ON E.v = V.v
            JOIN Ri%s W ON E.w = W.v
            WHERE V.r != W.r;
        ', i, i);

        GET DIAGNOSTICS rowcount = ROW_COUNT;

        DROP TABLE E;
        ALTER TABLE T RENAME TO E;

        EXIT WHEN rowcount = 0;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS compose_representatives;
CREATE FUNCTION compose_representatives() RETURNS void AS $$
DECLARE
    curr INT;
    A INT := 1;
    B INT := 0;
    α INT;
    β INT;
BEGIN
    SELECT MAX(id) INTO curr FROM S;

    WHILE curr > 1 LOOP
        SELECT S.a, S.b INTO α, β FROM S WHERE S.id = curr;

        A := axb(A, α, 0);
        B := axb(A, β, B);

        EXECUTE format('
            DROP TABLE IF EXISTS T;
            CREATE TABLE T AS
            SELECT L.v, COALESCE(R.r, axb(%s, L.r, %s)) AS r
            FROM Ri%s L
            LEFT JOIN Ri%s R ON L.r = R.v;
        ', A, B, curr - 1, curr);

        EXECUTE format('DROP TABLE Ri%s;', curr);
        EXECUTE format('DROP TABLE Ri%s;', curr - 1);
        EXECUTE format('ALTER TABLE T RENAME TO Ri%s;', curr - 1);

        curr := curr - 1;
    END LOOP;

    DROP TABLE IF EXISTS Result;
    ALTER TABLE Ri1 RENAME TO Result;
END;
$$ LANGUAGE plpgsql;


DO $$
BEGIN
    PERFORM contract_graph();
    PERFORM compose_representatives();
END $$;

select * from result;