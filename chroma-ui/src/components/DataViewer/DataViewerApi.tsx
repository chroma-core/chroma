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
  fetch(`/api/projections/` + projection_set_id, {
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
  var t1 = performance.now()
  var t2: any = null
  var t3: any = null
  var t4: any = null
  fetch(`/api/datapoints/` + project_id + "&page=" + page_id, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
    .then(response => {
      t2 = performance.now()
      // console.log(`fetch: ${(t2 - t1) / 1000} seconds.`);
      return response.text()
    })
    .then((response) => {
      t3 = performance.now()
      // console.log(`unpack: ${(t3 - t2) / 1000} seconds.`);
      worker.postMessage(response)
      worker.onmessage = (e: MessageEvent) => {
        var { data } = e
        t4 = performance.now()
        // console.log(`process: ${(t4 - t3) / 1000} seconds.`);
        cb(data, data.numberOfDatapoints, page_id)

      }
    })
    .catch((error) => {
      cb({ error: true, message: error }, 0, 0)
    })
}