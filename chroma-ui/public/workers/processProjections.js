self.onmessage = (message) => {
    var { data } = message
    var projectionsObject = {}

    var datapointsCopy = Object.assign({}, data.datapoints)

    data.projections.projections.map((proj) => {
        projectionsObject[proj.id] = {
            id: proj.id,
            x: proj.x,
            y: proj.y,
            datapoint_id: proj.embedding.datapoint_id,
            target: proj.target
        }

        if (data.projections.setType == 'object') {
            let inferenceDatapointId = Object.values(datapointsCopy).find(d => d.annotations[0].id == proj.target)
            datapointsCopy[inferenceDatapointId.id].projection_id = proj.id
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
