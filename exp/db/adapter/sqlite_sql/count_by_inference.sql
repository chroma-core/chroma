    SELECT inferences, count(*)
      FROM train_embeddings
  GROUP BY inferences;
