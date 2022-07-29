
import { atom, useAtom } from 'jotai'
import { atomWithQuery } from 'jotai/query'
import { normalize, schema } from 'normalizr'
import { Suspense, useEffect } from 'react'
import { Text, Grid, GridItem } from '@chakra-ui/react'

const inference = new schema.Entity(
  'inferences',
  {},
  {
    idAttribute: 'id',
    processStrategy: (entity, parent) => ({ ...entity, datapoint: parent.id })
  }
);

const label = new schema.Entity(
  'labels',
  {},
  {
    idAttribute: 'id',
    processStrategy: (entity, parent) => ({ ...entity, datapoint: parent.id })
  }
);

const resource = new schema.Entity(
  'resources',
  {},
  {
    idAttribute: 'id',
    processStrategy: (entity, parent) => ({ ...entity, datapoint: parent.id })
  }
);

const projection = new schema.Entity(
  'projections',
  {},
  {
    idAttribute: 'id',
    processStrategy: (entity, parent) => ({ ...entity, datapoint: parent.id })
  }
);

const addDatapointsMergeStrategy = (entityA: any, entityB: any) => {
  return {
    ...entityA,
    ...entityB,
    datapoints: [...(entityA.datapoints || []), ...(entityB.datapoints || [])],
  };
};

const tag = new schema.Entity(
  'tags',
  {},
  {
    idAttribute: 'id',
    mergeStrategy: addDatapointsMergeStrategy,
    processStrategy: (entity, parent) => ({ ...entity, datapoints: [parent.id] })
  }
);

const dataset = new schema.Entity(
  'datasets',
  {},
  {
    idAttribute: 'id',
    mergeStrategy: addDatapointsMergeStrategy,
    processStrategy: (entity, parent) => ({ ...entity, datapoints: [parent.id] })
  }
);

const addCategoryProcessStrategy = (entity: any, parent: any) => {
  if (parent.annotations.find((a: any) => a.category_id == entity.id) !== undefined) return { ...entity, datapoints: [parent.id] }
  else return { ...entity }
}

const category = new schema.Entity(
  'categories',
  {},
  {
    idAttribute: 'id',
    mergeStrategy: addDatapointsMergeStrategy,
    processStrategy: addCategoryProcessStrategy
  }
);

const datapoint = new schema.Entity('datapoints', {
  dataset: dataset,
  inference: inference,
  label: label,
  tags: [tag],
  resource: resource,
  categories: [category],
  projection: projection,
});


const preprocess = (datapoints: any) => {
  datapoints.map((dp: any) => {

    // our HABTM models are fetched oddly, this is a hack to fix that
    // @ts-ignore
    let newTags = []
    if (dp.tags.length > 0) {
      dp.tags.map((t: any) => {
        newTags.push(t.tag)
      })
      // @ts-ignore
      dp.tags = newTags
    }

    // open up our inference and label json
    const labelData = JSON.parse(dp.label.data)
    dp.annotations = labelData.annotations
    dp.categories = labelData.categories

    // parse metadata
    dp.metadata = JSON.parse(dp.metadata_)

    // inject projection for now..........
    dp.projection = { id: dp.id, x: Math.random() * 100, y: Math.random() * 100 }
  })

  return datapoints
}


export function getDatapointsForProject(project_id: number, cb: (data: any) => void) {
  fetch(`/api/datapoints/` + project_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(res => res.json())
    .then(res => {
      const normalizedData = normalize(preprocess(res.datapoints), [datapoint]);
      cb(normalizedData)
    })
    .catch((error) => {
      cb({ error: true, message: error })
      // Only network error comes here
    });
}

interface Annotation {
  id: number
  category_id: number
  area: number
  bbox: number[]
  iscrowd: number
  image_id: number
  segmentation: number[]
}

interface Datapoint {
  dataset: number
  id: number
  inference: number
  label: number
  metadata: {}
  resource: number
  tags: number[]
  annotations: Annotation[]
  projection: Projection
}

interface Dataset {
  id: number
  name: string
  datapoints: number[]
}

interface Projection {
  id: number
  x: number
  y: number
  datapoint: number
}

interface Inference {
  id: number
  data: string
  datapoint: number
}

interface Label {
  id: number
  data: string
  datapoint: number
}

interface Resource {
  id: number
  uri: string
  datapoint: number
}

interface Tag {
  id: number
  name: string
  datapoints: number[]
}

interface Category {
  id: number
  name: string
  datapoints: number[]
}

interface NormalizeData {
  entities: {
    datapoints: { [key: number]: Datapoint }
    datasets: { [key: number]: Dataset }
    inferences: { [key: number]: Inference }
    labels: { [key: number]: Label }
    resources: { [key: number]: Resource }
    tags: { [key: number]: Tag }
    categories: { [key: number]: Category }
    projections: { [key: number]: Projection }
  }
}

const datapointsAtom = atom<{ [key: number]: Datapoint }>({})
const datasetsAtom = atom<{ [key: number]: Dataset }>({})
const labelsAtom = atom<{ [key: number]: Label }>({})
const tagsAtom = atom<{ [key: number]: Tag }>({})
const resourcesAtom = atom<{ [key: number]: Resource }>({})
const inferencesAtom = atom<{ [key: number]: Inference }>({})
const categoriesAtom = atom<{ [key: number]: Category }>({})
const projectionsAtom = atom<{ [key: number]: Projection }>({})
const selectedDatapointsAtom = atom<number[]>([])

const DataFetchTest = () => {
  const [datapoints, updatedatapoints] = useAtom(datapointsAtom)
  const [labels, updatelabels] = useAtom(labelsAtom)
  const [tags, updatetags] = useAtom(tagsAtom)
  const [resources, updateresources] = useAtom(resourcesAtom)
  const [inferences, updateinferences] = useAtom(inferencesAtom)
  const [datasets, updatedatasets] = useAtom(datasetsAtom)
  const [categories, updatecategories] = useAtom(categoriesAtom)
  const [projections, updateprojections] = useAtom(projectionsAtom)

  const hydrateAtoms = (normalizedData: NormalizeData) => {
    updatedatapoints(normalizedData.entities.datapoints)
    updatedatasets(normalizedData.entities.datasets)
    updatelabels(normalizedData.entities.labels)
    updatetags(normalizedData.entities.tags)
    updateresources(normalizedData.entities.resources)
    updateinferences(normalizedData.entities.inferences)
    updatecategories(normalizedData.entities.categories)
    updateprojections(normalizedData.entities.projections)
  }

  useEffect(() => {
    getDatapointsForProject(2, hydrateAtoms)
  }, [])


  return (
    <Suspense fallback="Loading">

      {/* <Grid templateColumns='repeat(6, 1fr)' gap={6}>
        <GridItem w='100%'>
          <Text>Annotations</Text>
          {(Object.keys(datapoints).length > 0) ? 
          Object.keys(datapoints[70002].annotations).map(function(keyName, keyIndex) {
            if (keyIndex > 100) return
            return (
              // @ts-ignore
              <Text key={keyName}>{categories[datapoints[70002].annotations[keyName].category_id].name}</Text>
            )
          })
          : null}
        </GridItem>
      </Grid> */}

      <Grid templateColumns='repeat(6, 1fr)' gap={6}>
        <GridItem w='100%'>
          <Text>Datapoints</Text>
          {Object.keys(datapoints).map(function (keyName, keyIndex) {
            if (keyIndex > 100) return
            return (
              // @ts-ignore
              <Text key={keyName}>{keyName} - {keyIndex}</Text>
            )
          })}
        </GridItem>
        <GridItem w='100%'>
          <Text>Datasets</Text>
          {Object.keys(datasets).map(function (keyName, keyIndex) {
            return (
              // @ts-ignore
              <Text key={keyName}>{keyName} - {keyIndex}</Text>
            )
          })}
        </GridItem>
        <GridItem w='100%'>
          <Text>Labels</Text>
          {Object.keys(labels).map(function (keyName, keyIndex) {
            if (keyIndex > 100) return
            return (
              // @ts-ignore
              <Text key={keyName}>{keyName} - {keyIndex}</Text>
            )
          })}
        </GridItem>
        <GridItem w='100%'>
          <Text>Resources</Text>
          {Object.keys(resources).map(function (keyName, keyIndex) {
            if (keyIndex > 100) return
            return (
              // @ts-ignore
              <Text key={keyName}>{keyName} - {keyIndex}</Text>
            )
          })}
        </GridItem>
        <GridItem w='100%'>
          <Text>Tags</Text>
          {Object.keys(tags).map(function (keyName, keyIndex) {
            if (keyIndex > 100) return
            return (
              // @ts-ignore
              <Text key={keyName}>{keyName} - {keyIndex}</Text>
            )
          })}
        </GridItem>
        <GridItem w='100%'>
          <Text>Categories</Text>
          {Object.keys(categories).map(function (keyName, keyIndex) {
            if (keyIndex > 100) return
            return (
              // @ts-ignore
              <Text key={keyName}>{keyName} - {keyIndex}</Text>
            )
          })}
        </GridItem>
        <GridItem w='100%'>
          {/* <Text>Inferences</Text>
            {Object.keys(inferences).map(function(keyName, keyIndex) {
              if (keyIndex > 100) return
              return (
                // @ts-ignore
                <Text key={keyName}>{keyName} - {keyIndex}</Text>
              )
            })} */}
        </GridItem>
      </Grid>
    </Suspense>
  )
}


export default DataFetchTest