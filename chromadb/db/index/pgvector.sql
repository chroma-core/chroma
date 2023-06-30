SELECT query_id,
    ARRAY_AGG(knn.uuid),
    ARRAY_AGG(knn.distance)
FROM (
        VALUES (VECTOR('[2, 9, 5, 2, 9, 1, 2, 4, 9, 1]'), 1),
            (VECTOR('[1, 3, 5, 7, 9, 10, 22, 3, 9, 10]'), 2)
    ) AS tq (query, query_id),
    LATERAL(
        SELECT e.uuid,
            e.embedding <->tq.query AS distance
        FROM embeddings10 e
        ORDER BY "distance"
        LIMIT 3 -- LIMIT K
    ) knn
GROUP BY query_id
ORDER BY query_id
