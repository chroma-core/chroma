// import { schema } from "normalizr";

// const inference = new schema.Entity(
//   'inferences',
//   {},
//   {
//     idAttribute: 'id',
//   }
// );

// const label = new schema.Entity(
//   'labels',
//   {},
//   {
//     idAttribute: 'id',
//   }
// );

// const resource = new schema.Entity(
//   'resources',
//   {},
//   {
//     idAttribute: 'id',
//   }
// );

// const projection = new schema.Entity(
//   'projections',
//   {},
//   {
//     idAttribute: 'id',
//   }
// );

// const addDatapointsMergeStrategy = (entityA: any, entityB: any) => {
//   return {
//     ...entityA,
//     ...entityB,
//     datapoints: [...(entityA.datapoints || []), ...(entityB.datapoints || [])],
//   };
// };

// const tag = new schema.Entity(
//   'tags',
//   {},
//   {
//     idAttribute: 'id',
//     // mergeStrategy: addDatapointsMergeStrategy,
//     // processStrategy: (entity, parent) => ({ ...entity, datapoints: [parent.id] })
//   }
// );

// const dataset = new schema.Entity(
//   'datasets',
//   {},
//   {
//     idAttribute: 'id',
//     // mergeStrategy: addDatapointsMergeStrategy,
//     // processStrategy: (entity, parent) => ({ ...entity, datapoints: [parent.id] })
//   }
// );

// // const addCategoryProcessStrategy = (entity: any, parent: any) => {
// //   if (parent.annotations.find((a: any) => a.category_id == entity.id) !== undefined) return { ...entity, datapoints: [parent.id] }
// //   else return { ...entity }
// // }

// // const category = new schema.Entity(
// //   'categories',
// //   {},
// //   {
// //     idAttribute: 'id',
// //     mergeStrategy: addDatapointsMergeStrategy,
// //     processStrategy: addCategoryProcessStrategy
// //   }
// // );
// const category = new schema.Entity(
//   'categories',
//   {},
//   {
//     idAttribute: 'id',
//   }
// );

// // export const datapoint = new schema.Entity('datapoints', {
// //   dataset: dataset,
// //   inference: inference,
// //   label: label,
// //   tags: [tag],
// //   resource: resource,
// //   categories: [category],
// //   projection: projection,
// // });
// export const datapoint = new schema.Entity('datapoints', {});

// // export const responseSchema = new schema.Entity('response', {
// //   datapoints: [datapoint],
// //   datasets: [dataset],
// //   inferences: [inference],
// //   labels: [label],
// //   resources: [resource],
// //   projection: [projection],
// // });

// export const responseSchema = {
//   datapoints: [datapoint],
//   datasets: [dataset],
//   inferences: [inference],
//   labels: [label],
//   resources: [resource],
//   projections: [projection],
//   tags: [tag],
//   categories: [category]
// }

export { }