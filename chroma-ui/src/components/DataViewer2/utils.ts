import { Datapoint } from "./types"

// export const preprocess = (datapoints: any) => {
//   datapoints.map((dp: any) => {

//     // our HABTM models are fetched oddly, this is a hack to fix that
//     // @ts-ignore
//     let newTags = []
//     if (dp.tags.length > 0) {
//       dp.tags.map((t: any) => {
//         newTags.push(t.tag)
//       })
//       // @ts-ignore
//       dp.tags = newTags
//     }

//     // open up our inference and label json
//     const labelData = JSON.parse(dp.label.data)
//     dp.annotations = labelData.annotations
//     //dp.categories = labelData.categories

//     // parse metadata
//     // dp.metadata = JSON.parse(dp.metadata_)

//     // inject projection for now..........
//     dp.projection = { id: dp.id, x: Math.random() * 10, y: Math.random() * 10 }
//   })

//   return datapoints
// }

// export const preprocess2 = (returnedData: any) => {
//   let categoriesObject: { [key: number]: {} } = {}
//   let datapointsObject: { [key: number]: Datapoint } = {}
//   let datasetsObject: { [key: number]: {} } = {}
//   let labelsObject: { [key: number]: {} } = {}
//   let resourcesObject: { [key: number]: {} } = {}
//   let tagsObject: { [key: number]: {} } = {}
//   let projectionsObject: { [key: number]: {} } = {}
//   let inferencesObject: { [key: number]: {} } = {}

//   const { datapoints, labels, resources, inferences, datasets, tags } = returnedData;

//   datasets.forEach((dataset: any) => {
//     datasetsObject[dataset.id] = {
//       ...dataset,
//       datapoint_ids: []
//     }

//     let categoriesData = JSON.parse(dataset.categories)

//     categoriesData.forEach((category: any) => {
//       if (categoriesObject[category.id] === undefined) {
//         categoriesObject[category.id] = { ...category }
//       }
//     })
//   })

//   tags.forEach((tag: any) => {
//     tagsObject[tag.id] = {
//       ...tag,
//       datapoint_ids: []
//     }
//   })

//   datapoints.forEach((datapoint: any) => {
//     projectionsObject[datapoint.id] = { id: datapoint.id, x: Math.random() * 10, y: Math.random() * 10, datapoint_id: datapoint.id }
//     datapointsObject[datapoint.id] = {
//       ...datapoint,
//       projection_id: datapoint.id,
//       tags: []
//     }

//     // @ts-ignore
//     datasetsObject[datapoint.dataset_id].datapoint_ids.push(datapoint.id)
//   })

//   let annsIdsToAdd = {}
//   labels.forEach((label: any) => {
//     const labelData = JSON.parse(label.data)
//     labelsObject[label.id] = {
//       ...label,
//       data: labelData
//     }

//     datapointsObject[label.datapoint_id].annotations = labelData.annotations

//     labelData.annotations.forEach((annotation: any) => {
//       const categoryId = annotation.category_id
//       // @ts-ignore
//       if (annsIdsToAdd[categoryId] === undefined) annsIdsToAdd[categoryId] = new Set()
//       // @ts-ignore
//       annsIdsToAdd[categoryId].add(label.datapoint_id)
//     })

//   })

//   Object.keys(annsIdsToAdd).map((c, i) => {
//     // @ts-ignore
//     const dps = (annsIdsToAdd[c]).keys()
//     // @ts-ignore
//     categoriesObject[c].datapoint_ids = [...dps]
//   })

//   resources.forEach((resource: any) => {
//     resourcesObject[resource.id] = {
//       ...resource
//     }
//   })

//   inferences.forEach((inference: any) => {
//     inferencesObject[inference.id] = {
//       ...inference,
//       data: JSON.parse(inference.data)
//     }
//   })

//   return {
//     categories: categoriesObject,
//     labels: labelsObject,
//     datapoints: datapointsObject,
//     datasets: datasetsObject,
//     resources: resourcesObject,
//     tags: tagsObject,
//     inferences: inferencesObject,
//     projections: projectionsObject
//   }
// }