self.onmessage = (message) => {
    var { data } = message
    var dataRead = JSON.parse(data)

    // create data structures
    let categoriesObject = {} // { [key: number]: {} } =
    let datapointsObject = {} // { [key: number]: Datapoint } =
    let datasetsObject = {} // { [key: number]: {} } =
    let labelsObject = {} // { [key: number]: {} } =
    let resourcesObject = {} // { [key: number]: {} } =
    let tagsObject = {} // { [key: number]: {} } =
    let inferencesObject = {} // { [key: number]: {} } =

    let labeldatasetsObject = {} // { [key: number]: {} } =
    let labelCategoriesObject = {}

    let inferencedatasetsObject = {} // { [key: number]: {} } =
    let inferenceCategoriesObject = {}

    // destructure data out of json response
    const { datapoints, labels, resources, inferences, datasets, tags } = dataRead;

    // load datasets object and unpack valid categories from dataset categories column
    datasets.forEach((dataset) => {
        datasetsObject[dataset.id] = {
            ...dataset,
            datapoint_ids: []
        }
        labeldatasetsObject[dataset.id] = {
            ...dataset,
            datapoint_ids: []
        }
        inferencedatasetsObject[dataset.id] = {
            ...dataset,
            datapoint_ids: []
        }

        let categoriesData = JSON.parse(dataset.categories)

        categoriesData.forEach((category) => {
            if (categoriesObject[category.id] === undefined) {
                categoriesObject[category.id] = { ...category }
                labelCategoriesObject[category.id] = { ...category }
                inferenceCategoriesObject[category.id] = { ...category }
            }
        })
    })

    // load tags object and fill in the datapoints that have that tag
    tags.forEach((tag) => {
        tagsObject[tag.tag.id] = {
            ...tag.tag,
            datapoint_ids: ((tagsObject[tag.tag.id]?.datapoint_ids === undefined ? [tag.right_id] : [...tagsObject[tag.tag.id].datapoint_ids, tag.right_id]))
        }
    })

    metadataFilters = {}

    // load datapoints object, and also use datapoint metadata to create custom filters
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

        // this is used to stub out projection data
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

    // add our cross link from tags back onto datapoints
    Object.values(tagsObject).map(function (tag) {
        tag.datapoint_ids.map(dpid => {
            datapointsObject[dpid].tag_ids.push(tag.id)
        })
    })

    // take datapoint annotation data and add it to the category datapoint list
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

        // @ts-ignore
        // labeldatasetsObject[datapointsObject[label.datapoint_id].dataset_id].datapoint_ids.push(label.id)
    })
    Object.keys(annsIdsToAdd).map((c) => {
        // @ts-ignore
        const dps = (annsIdsToAdd[c]).keys()
        // @ts-ignore
        categoriesObject[c].datapoint_ids = [...dps]
    })

    // load resources object
    resources.forEach((resource) => {
        resourcesObject[resource.id] = {
            ...resource
        }
    })

    // load inferences object
    inferences.forEach((inference) => {
        const inferenceData = JSON.parse(inference.data)
        inferencesObject[inference.id] = {
            ...inference,
            data: inferenceData
        }

        if (inferenceData.annotations) datapointsObject[inference.datapoint_id].inferences = inferenceData.annotations
    })

    // create the object version of things for labels
    // has to come after datapoints object and its annotations have been filled
    var i = 0
    var j = 0
    let labelDatapointsObject = {}
    let labelResourcesObject = {}
    Object.values(datapointsObject).map(dp => {
        dp.annotations.map(ann => {
            let hasBoundingBoxes = (ann.bbox !== undefined)
            if (hasBoundingBoxes) {
                labelResourcesObject[j] = {
                    id: j,
                    uri: resourcesObject[dp.id].uri
                }
                labelDatapointsObject[i] = {
                    annotations: [ann],
                    dataset_id: dp.dataset_id,
                    id: i,
                    inferences: [],
                    metadata: {},
                    tag_ids: [],
                    resource_id: j
                }
                labeldatasetsObject[dp.dataset_id].datapoint_ids.push(i)
                i++
                j++
            }

        })
    })

    let annsLabelIdsToAdd = {}
    Object.values(labelDatapointsObject).map(labelDp => {
        let categoryId = labelDp.annotations[0].category_id
        if (annsLabelIdsToAdd[categoryId] === undefined) annsLabelIdsToAdd[categoryId] = new Set()
        annsLabelIdsToAdd[categoryId].add(labelDp.id)
    })

    Object.keys(annsLabelIdsToAdd).map((c) => {
        // @ts-ignore
        const dps = (annsLabelIdsToAdd[c]).keys()
        // @ts-ignore
        labelCategoriesObject[c].datapoint_ids = [...dps]
    })

    // if we dont have any bounding boxes... wipe out the other data structures
    if (Object.values(labelDatapointsObject).length === 0) {
        labelCategoriesObject = {}
        labelResourcesObject = {}
        labeldatasetsObject = {}
    }

    // create the object version of things for inferences
    // has to come after datapoints object and its annotations have been filled
    var i = 0
    var j = 0
    let inferenceDatapointsObject = {}
    let inferenceResourcesObject = {}
    Object.values(datapointsObject).map(dp => {
        dp.inferences.map(ann => {
            let hasBoundingBoxes = (ann.bbox !== undefined)
            if (hasBoundingBoxes) {
                inferenceResourcesObject[j] = {
                    id: j,
                    uri: resourcesObject[dp.id].uri
                }
                inferenceDatapointsObject[i] = {
                    annotations: [ann],
                    dataset_id: dp.dataset_id,
                    id: i,
                    inferences: [],
                    metadata: {},
                    tag_ids: [],
                    resource_id: j,
                    inference: true
                }
                inferencedatasetsObject[dp.dataset_id].datapoint_ids.push(i)
                i++
                j++
            }

        })
    })

    let annsInferenceIdsToAdd = {}
    Object.values(inferenceDatapointsObject).map(inferenceDp => {
        let categoryId = inferenceDp.annotations[0].category_id
        if (annsInferenceIdsToAdd[categoryId] === undefined) annsInferenceIdsToAdd[categoryId] = new Set()
        annsInferenceIdsToAdd[categoryId].add(inferenceDp.id)
    })

    Object.keys(annsInferenceIdsToAdd).map((c) => {
        // @ts-ignore
        const dps = (annsInferenceIdsToAdd[c]).keys()
        // @ts-ignore
        inferenceCategoriesObject[c].datapoint_ids = [...dps]
    })

    // if we dont have any bounding boxes... wipe out the other data structures
    if (Object.values(inferenceDatapointsObject).length === 0) {
        inferenceCategoriesObject = {}
        inferenceResourcesObject = {}
        inferencedatasetsObject = {}
    }



    // let labelCategoriesObject = categoriesObject
    let labelLabelsObject = {}
    let inferenceLabelsObject = {}
    let labelTagsObject = {}
    let labelInferencesObject = {}
    let labelMetadataFiltersObject = {}

    self.postMessage({
        // datapoints stuff
        categories: categoriesObject,
        labels: labelsObject,
        datapoints: datapointsObject,
        datasets: datasetsObject,
        resources: resourcesObject,
        tags: tagsObject || {},
        inferences: inferencesObject,
        numberOfDatapoints: datapoints.length,
        metadataFilters: metadataFilters,
        // labels stuff
        labelCategories: labelCategoriesObject,
        labelLabels: labelLabelsObject,
        labelDatapoints: labelDatapointsObject,
        labelDatasets: labeldatasetsObject,
        labelResources: labelResourcesObject,
        labelTags: labelTagsObject,
        labelInferences: labelInferencesObject,
        labelMetadataFilters: labelMetadataFiltersObject,
        // inferences stuff
        inferenceCategories: inferenceCategoriesObject,
        // labelLabels: labelLabelsObject,
        inferenceDatapoints: inferenceDatapointsObject,
        inferenceDatasets: inferencedatasetsObject,
        inferenceResources: inferenceResourcesObject,
        inferenceLabels: inferenceLabelsObject,
        // labelTags: labelTagsObject,
        // labelInferences: labelInferencesObject,
        // labelMetadataFilters: labelMetadataFiltersObject,
    })
}