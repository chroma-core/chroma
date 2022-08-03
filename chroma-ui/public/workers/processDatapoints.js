self.onmessage = (message) => {
    var { data } = message
    var dataRead = JSON.parse(data)
    let categoriesObject = {} // { [key: number]: {} } =
    let datapointsObject = {} // { [key: number]: Datapoint } =
    let datasetsObject = {} // { [key: number]: {} } =
    let labelsObject = {} // { [key: number]: {} } =
    let resourcesObject = {} // { [key: number]: {} } =
    let tagsObject = {} // { [key: number]: {} } =
    //let projectionsObject = {} // { [key: number]: {} } =
    let inferencesObject = {} // { [key: number]: {} } =

    const { datapoints, labels, resources, inferences, datasets, tags } = dataRead;

    datasets.forEach((dataset) => {
        datasetsObject[dataset.id] = {
            ...dataset,
            datapoint_ids: []
        }

        let categoriesData = JSON.parse(dataset.categories)

        categoriesData.forEach((category) => {
            if (categoriesObject[category.id] === undefined) {
                categoriesObject[category.id] = { ...category }
            }
        })
    })

    tags.forEach((tag) => {
        tagsObject[tag.tag.id] = {
            ...tag.tag,
            datapoint_ids: ((tagsObject[tag.tag.id]?.datapoint_ids === undefined ? [tag.right_id] : [...tagsObject[tag.tag.id].datapoint_ids, tag.right_id]))
        }
    })

    metadataFilters = {}

    datapoints.forEach((datapoint) => {

        // build the metadata dict
        if (datapoint.metadata_ == '') datapoint.metadata_ = "{}"
        var datapointaMetadata = JSON.parse(datapoint.metadata_)
        Object.keys(datapointaMetadata).map(key => {
            if (metadataFilters[key] === undefined) metadataFilters[key] = { name: key, options: {}, type: 0, linkedAtom: {} }
            if (metadataFilters[key].linkedAtom[datapointaMetadata[key]] === undefined) metadataFilters[key].linkedAtom[datapointaMetadata[key]] = { datapoint_ids: [] }

            metadataFilters[key].options[datapointaMetadata[key]] = {
                id: datapointaMetadata[key],
                visible: true,
                color: "#333333",
            }
            metadataFilters[key].linkedAtom[datapointaMetadata[key]] = {
                id: datapointaMetadata[key],
                name: datapointaMetadata[key],
                datapoint_ids: [...metadataFilters[key].linkedAtom[datapointaMetadata[key]].datapoint_ids, datapoint.id]
            }
        })

        //projectionsObject[datapoint.id] = { id: datapoint.id, x: Math.random() * 10, y: Math.random() * 10, datapoint_id: datapoint.id }
        datapointsObject[datapoint.id] = {
            ...datapoint,
            tag_ids: [],
            inferences: [],
            annotations: [],
            metadata: datapointaMetadata
        }

        // @ts-ignore
        datasetsObject[datapoint.dataset_id].datapoint_ids.push(datapoint.id)
    })

    Object.values(tagsObject).map(function (tag) {
        tag.datapoint_ids.map(dpid => {
            datapointsObject[dpid].tag_ids.push(tag.id)
        })
    })

    let annsIdsToAdd = {}
    labels.forEach((label) => {
        const labelData = JSON.parse(label.data)
        labelsObject[label.id] = {
            ...label,
            data: labelData
        }

        datapointsObject[label.datapoint_id].annotations = labelData.annotations

        labelData.annotations.forEach((annotation) => {
            const categoryId = annotation.category_id
            // @ts-ignore
            if (annsIdsToAdd[categoryId] === undefined) annsIdsToAdd[categoryId] = new Set()
            // @ts-ignore
            annsIdsToAdd[categoryId].add(label.datapoint_id)
        })

    })

    Object.keys(annsIdsToAdd).map((c) => {
        // @ts-ignore
        const dps = (annsIdsToAdd[c]).keys()
        // @ts-ignore
        categoriesObject[c].datapoint_ids = [...dps]
    })

    resources.forEach((resource) => {
        resourcesObject[resource.id] = {
            ...resource
        }
    })

    inferences.forEach((inference) => {
        const inferenceData = JSON.parse(inference.data)
        inferencesObject[inference.id] = {
            ...inference,
            data: inferenceData
        }

        if (inferenceData.annotations) datapointsObject[inference.datapoint_id].inferences = inferenceData.annotations
    })

    self.postMessage({
        categories: categoriesObject,
        labels: labelsObject,
        datapoints: datapointsObject,
        datasets: datasetsObject,
        resources: resourcesObject,
        tags: tagsObject || {},
        inferences: inferencesObject,
        numberOfDatapoints: datapoints.length,
        metadataFilters: metadataFilters
    })
}