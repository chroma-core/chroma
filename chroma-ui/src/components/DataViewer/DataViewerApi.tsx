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
  fetch(`/api/projection_set_data_viewer/` + projection_set_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(res => res.json())
    .then(res => {
      cb(res.projections)
    })
    .catch((error) => {
      cb({ error: true, message: error })
      // Only network error comes here
    });
}

export function getTotalDatapointsToFetch(project_id: number, cb: (res: any) => void) {
  fetch(`/api/datapoints_count/` + project_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(res => res.json())
    .then(res => {
      cb(res)
    })
    .catch((error) => {
      cb({ error: true, message: error })
      // Only network error comes here
    });
}

const worker: Worker = new Worker('/workers/processDatapoints.js')

export function getDatapointsForProject(project_id: number, page_id: number, cb: (data: any, datalen: number, prevPage: number) => void) {
  fetch(`/api/datapoints/` + project_id + "&page=" + page_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(response => {
      return response.text()
    })
    .then((response) => {
      worker.postMessage(response)
      worker.onmessage = (e: MessageEvent) => {
        var { data } = e
        cb(data, data.numberOfDatapoints, page_id)
      }
    })
    .catch((error) => {
      cb({ error: true, message: error }, 0, 0)
    })
}