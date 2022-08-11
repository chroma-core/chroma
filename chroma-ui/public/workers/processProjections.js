self.onmessage = (message) => {
    var { data } = message
    var projectionsObject = {}
    var datapointsCopy = Object.assign({}, data.datapoints)
    data.projections.map((proj) => {
        projectionsObject[proj.id] = {
            id: proj.id,
            x: proj.x,
            y: proj.y,
            datapoint_id: proj.embedding.datapoint_id
        }
        datapointsCopy[proj.embedding.datapoint_id].projection_id = proj.id
    })

    self.postMessage({
        datapoints: datapointsCopy,
        projections: projectionsObject
    })
}
