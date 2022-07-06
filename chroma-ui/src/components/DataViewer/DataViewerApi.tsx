export const GetProjectAndProjectionSets = `
query getProjectionSets($filter: FilterProjectionSets!, $projectId: ID!) {
  projectionSets(filter: $filter) {
    id
    createdAt
    updatedAt
  }
  project(id: $projectId) {
    id
    name
    datasets {
      id
      name
    }
  }
}
`

export function getProjectionsForProjectionSet(projection_set_id: number, cb: (projections: any) => void) {
  console.log('running getProjectionsForProjectionSet')
  console.time('getProjectionsForProjectionSet')
  fetch(`/api/projection_set_data_viewer/` + projection_set_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(res => res.json())
    .then(res => {
      console.timeEnd('getProjectionsForProjectionSet')
      cb(res.projections)
    })
    .catch((error) => {
      cb({ error: true, message: error })
      // Only network error comes here
    });
}

export function getDatapointsForProject(project_id: number, cb: (data: any) => void) {
  console.log('running getDatapointsForProject')
  console.time('getDatapointsForProject')
  fetch(`/api/datapoints/` + project_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(res => res.json())
    .then(res => {
      console.timeEnd('getDatapointsForProject')
      cb(res)
    })
    .catch((error) => {
      cb({ error: true, message: error })
      // Only network error comes here
    });
}