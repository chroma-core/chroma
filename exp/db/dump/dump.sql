select p.id as projection_id,
       e.id as embedding_id,
       dp.id as datapoint_id,
       inf.id as inference_id,
       p.projection_set_id,
       e.embedding_set_id,
       ps.setType as projection_set_type,
       pr.name as project_name,
       inf.data as inference,
       e.data as data
  from embeddings e
  join datapoints dp on dp.id = e.datapoint_id,
       embedding_sets es on es.id = e.embedding_set_id,
       inferences inf on inf.datapoint_id = dp.id,
       labels l on l.datapoint_id = dp.id,
       projections p on p.embedding_id = e.id,
       projection_sets ps on ps.id = p.projection_set_id,
       projects pr on pr.id = ps.project_id
