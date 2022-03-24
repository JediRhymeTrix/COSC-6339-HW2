DROP TABLE IF EXISTS cs44_graphsql_output;
CREATE TABLE cs44_graphsql_output AS
WITH RECURSIVE solution
(v1, v2, v3, t, d) AS
(
    SELECT i, j, 0 ,j, 1
    FROM wikiVote
    UNION ALL
    SELECT solution.v1, solution.v3, solution.t, wikiVote.j, solution.d+1
    FROM solution
    JOIN wikiVote ON wikiVote.i=solution.t
    WHERE d < 3
)
SELECT v1, v2, v3
FROM solution
WHERE v1 = t and v1 < v2 and v2 < v3 and d = 3
ORDER BY v1, v2, v3;