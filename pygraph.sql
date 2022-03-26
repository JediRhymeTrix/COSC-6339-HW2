DROP TABLE IF EXISTS cs44_graphsql_output;
CREATE TABLE cs44_graphsql_output AS
WITH RECURSIVE solution(v1, v2, v3, v4, t, d) AS (
    SELECT i, j, j, j, j, 1 
    FROM wikiVote 
    UNION ALL 
    SELECT solution.v1, solution.v2, solution.v4, solution.t, wikiVote.j, solution.d+1 
    FROM solution 
    JOIN wikiVote ON wikiVote.i = solution.t 
    WHERE 
    wikiVote.j in (SELECT j FROM wikiVote w WHERE w.i=v1) AND 
    wikiVote.j in (SELECT j FROM wikiVote w WHERE w.i=v2) AND 
    wikiVote.j in (SELECT j FROM wikiVote w WHERE w.i=v3) AND 
    wikiVote.j in (SELECT j FROM wikiVote w WHERE w.i=v4) AND 
    d<4-1) 
SELECT v1, v2, v4, t 
FROM solution 
WHERE v1 < v2 AND v2 < v4 AND v4<t
ORDER BY v1, v2, v4, t;
