self.onmessage = (message) => {
    var { data } = message
    var projectionsObject = {}

    var datapointsCopy = Object.assign({}, data.datapoints)

    let targetIdDatapointIdMap = {}
    if (data.projections.setType == 'object') {
        Object.values(datapointsCopy).forEach(d => targetIdDatapointIdMap[d.inferences[0].id] = d.id)
    }

    data.projections.projections.map((proj, index) => {
        projectionsObject[proj.id] = {
            id: proj.id,
            x: proj.x,
            y: proj.y,
            datapoint_id: proj.embedding.datapoint_id,
            target: proj.target
        }

        if (data.projections.setType == 'object') {
            datapointsCopy[targetIdDatapointIdMap[proj.target]].projection_id = proj.id
        } else {
            datapointsCopy[proj.embedding.datapoint_id].projection_id = proj.id
        }

    })

    self.postMessage({
        setType: data.projections.setType,
        datapoints: datapointsCopy,
        projections: projectionsObject
    })
}
