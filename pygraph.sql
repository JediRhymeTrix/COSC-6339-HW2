DROP TABLE IF EXISTS cs44_graphsql_output;
CREATE TABLE cs44_graphsql_output AS
WITH RECURSIVE solution(v1, v2, v3, t, d) AS (
    SELECT i, j, j, j, 1 
    FROM cliqueTest 
    UNION ALL 
    SELECT solution.v1, solution.v3, solution.t, cliqueTest.j, solution.d+1 
    FROM solution 
    JOIN cliqueTest ON cliqueTest.i = solution.t 
    WHERE 
    cliqueTest.j in (SELECT j FROM cliqueTest w WHERE w.i=v1) AND 
    cliqueTest.j in (SELECT j FROM cliqueTest w WHERE w.i=v2) AND 
    cliqueTest.j in (SELECT j FROM cliqueTest w WHERE w.i=v3) AND 
    d<3-1) 
SELECT v1, v3, t 
FROM solution 
WHERE v1 < v3 AND v3<t
ORDER BY v1, v3, t;
