CREATE TABLE A(F int, T int, V numeric);
CREATE TABLE B(F int, T int, V numeric);

INSERT INTO A VALUES
  (1, 1, 2), (1, 2, 3),
  (2, 1, 4), (2, 2, 5);

INSERT INTO B VALUES
  (1, 1, 7), (1, 2, 8),
  (2, 1, 9), (2, 2,10);

/* ----------- MM-join：A ⊕(⊙)⋈B ------------ */
SELECT
    a.f               AS f,          -- A 的行号 → 结果行号 i
    b.t               AS t,          -- B 的列号 → 结果列号 j
    SUM(a.v * b.v)    AS v           -- ⊕(⊙)：Σ (Aik ⊙ Bkj)
FROM A AS a
JOIN B AS b
  ON a.t = b.f                     -- A.T = B.F  ← 连接 k
GROUP BY
    a.f,                           -- = A.F
    b.t                            -- = B.T
ORDER BY 1,2;

SELECT
    a.f AS f,
    b.t AS t,
    MIN(a.v + b.v) AS v      -- ① ⊕ = MIN，⊙ = +
FROM A AS a
JOIN B AS b
  ON a.t = b.f
GROUP BY a.f, b.t
ORDER BY 1,2;
