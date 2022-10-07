    SELECT inference, count(*)
      FROM train_embeddings
  GROUP BY inference;
