import time
from sqlalchemy import func, select
from sqlalchemy.orm import joinedload, load_only
import models

from fastapi import APIRouter

router = APIRouter()

# we go directly to sqlalchemy and skip graphql for fetching projections and their related data
# because it massively cuts down on the time to return data to the DOM 
@router.get("/api/projections/{projection_set_id}")
async def get_projection_set_data_viewer(projection_set_id: str):
    print("get_projection_set_data_viewer!")
    async with models.get_session() as s:
        print("get_projection_set_data_viewer models.get_session!" + str(s))
        start = time.process_time()

        sql = (
            select(models.ProjectionSet)
                .where(models.ProjectionSet.id == int(projection_set_id))
                .options(
                    load_only(
                        models.ProjectionSet.id, 
                        models.ProjectionSet.setType, 
                    ))
                .options(joinedload(models.ProjectionSet.projections)
                    .options(
                        load_only(models.Projection.x, models.Projection.y, models.Projection.target), 
                        joinedload(models.Projection.embedding)
                            .options(load_only(models.Embedding.id, models.Embedding.datapoint_id))
                        )
                )
        )
        val = (await s.execute(sql)).scalars().first()

        elapsedtime = time.process_time() - start
        print("got projections in " + str(elapsedtime) + " seconds")

    return val

@router.get("/api/datapoints_count/{project_id}")
async def get_datapoints_data_viewer(project_id: str):
    async with models.get_session() as s:
        query = select(func.count(models.Datapoint.id)).filter(models.Datapoint.project_id == project_id)
        res = (await s.execute(query)).scalar()
    return res

@router.get("/api/datapoints/{project_id}&page={page_id}")#, response_class=UJSONResponse)
async def get_datapoints_data_viewer(project_id: str, page_id: int):
    async with models.get_session() as s:
        print("get_datapoints_data_viewer models.get_session! " + str(s))
        start = time.process_time()

        page_size = 10000
        offset = page_size * page_id 

        sql = (select(models.Datapoint).where(models.Datapoint.project_id == int(project_id))
                .options(
                    load_only(
                        models.Datapoint.id, 
                        models.Datapoint.resource_id, 
                        models.Datapoint.dataset_id,
                        models.Datapoint.metadata_
                    )
                )).limit(page_size).offset(offset)
        datapoints = (await s.execute(sql)).scalars().unique().all()

        datapoint_ids = []
        resource_ids = []
        dataset_list = {}

        for dp in datapoints:
            datapoint_ids.append(dp.id)
            resource_ids.append(dp.id)
            dataset_list[dp.dataset_id] = True # eg {4: True}, use this to prevent dupes
        
        # Labels
        sql = (select(models.Label).filter(models.Label.datapoint_id.in_(datapoint_ids)).options(load_only(models.Label.id, models.Label.data, models.Label.datapoint_id)))
        labels = (await s.execute(sql)).scalars().unique().all()

        # Resources
        sql = (select(models.Resource).filter(models.Resource.id.in_(resource_ids)).options(load_only(models.Resource.id, models.Resource.uri)))
        resources = (await s.execute(sql)).scalars().unique().all()

        # Inferences
        sql = (select(models.Inference).filter(models.Inference.datapoint_id.in_(datapoint_ids)).options(load_only(models.Inference.id, models.Inference.data, models.Inference.datapoint_id)))
        inferences = (await s.execute(sql)).scalars().unique().all()

        # Datasets
        sql = (select(models.Dataset).filter(models.Dataset.id.in_(dataset_list.keys())).options(load_only(models.Dataset.id, models.Dataset.name, models.Dataset.categories)))
        datasets = (await s.execute(sql)).scalars().unique().all()
        
        # Tags
        sql = (select(models.Tagdatapoint).filter(models.Tagdatapoint.right_id.in_(datapoint_ids)).options(joinedload(models.Tagdatapoint.tag)))
        tags = (await s.execute(sql)).scalars().unique().all()

        elapsedtime = time.process_time() - start
        print("got datapoints in " + str(elapsedtime) + " seconds")

    return ({
        'datapoints': datapoints,
        'labels': labels,
        'resources': resources,
        'inferences': inferences,
        'datasets': datasets,
        'tags': tags
    })