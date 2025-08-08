DROP TABLE IF EXISTS G;
CREATE TABLE G (
    v INT,
    w INT
);

INSERT INTO G VALUES
    (1, 2),
    (1, 4),
    (2, 3),
    (3, 4),
    (4, 5),
    (6, 7),
    (6, 8),
    (9, 9);

DROP TABLE IF EXISTS S;
CREATE TABLE S (
    id INT,
    a INT,
    b INT
);

DROP FUNCTION IF EXISTS axb;
CREATE FUNCTION axb(a INT, x INT, b INT) RETURNS INT AS $$
BEGIN
    RETURN (a * x + b) % 100000;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
