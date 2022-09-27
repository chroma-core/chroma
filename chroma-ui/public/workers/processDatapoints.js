//
// this webworker takes raw data from the server and loads it up into data structures that our frontend can consume
// 

self.onmessage = (message) => {
    var { data } = message
    var parsedJson = JSON.parse(data)
    const { datapoints, labels, resources, inferences, datasets, tags } = parsedJson;

    // we have 2 versions of every object... 1 for contexts and 1 for objects

    // create context structures
    let context__categoriesObject = {}
    let context__inferenceCategoriesObject = {}
    let context__datapointsObject = {}
    let context__datasetsObject = {}
    let context__labelsObject = {}
    let context__resourcesObject = {}
    let context__tagsObject = {}
    let context__inferencesObject = {}
    let context__metadataFilters = {}

    // create object data structures
    let object__inferenceCategoriesObject = {}
    let object__datapointsObject = {}
    let object__datasetsObject = {}
    let object__labelsObject = {}
    let object__resourcesObject = {}
    let object__tagsObject = {}
    let object__labelCategoriesObject = {}
    let object__metadataFiltersObject = {}


    datasets.forEach((dataset) => {

        // set up the basic dataset objects
        context__datasetsObject[dataset.id] = {
            ...dataset,
            datapoint_ids: []
        }
        object__datasetsObject[dataset.id] = {
            ...dataset,
            datapoint_ids: []
        }

        // set up the basic category objects
        let categoriesData = JSON.parse(dataset.categories)
        categoriesData.forEach((category) => {
            if (context__categoriesObject[category.id] === undefined) {
                context__categoriesObject[category.id] = { ...category }
                object__inferenceCategoriesObject[category.id] = { ...category }
                context__inferenceCategoriesObject[category.id] = { ...category }
                object__labelCategoriesObject[category.id] = { ...category }
            }
        })


    })

    // load tags object and fill in the datapoints that have that tag
    tags.forEach((tag) => {
        var datapointList

        // filter out cases where the tagdatapoint is actually on the label annotation and not the datapoint
        if ((tag.target === null)) {

            if (context__tagsObject[tag.tag.id]?.datapoint_ids === undefined) {
                datapointList = [tag.right_id]
            } else {
                datapointList = [...context__tagsObject[tag.tag.id].datapoint_ids, tag.right_id]
            }

            context__tagsObject[tag.tag.id] = {
                ...tag.tag,
                datapoint_ids: datapointList
            }

        }
    })

    // load datapoints object, and also use datapoint metadata to create custom filters
    datapoints.forEach((datapoint) => {

        // build the metadata dict
        if (datapoint.metadata_ == '') datapoint.metadata_ = "{}"
        var datapointaMetadata = JSON.parse(datapoint.metadata_)
        Object.keys(datapointaMetadata).map(key => {
            if (context__metadataFilters[key] === undefined) context__metadataFilters[key] = { name: key, options: {}, type: 0, linkedAtom: {} }
            if (context__metadataFilters[key].linkedAtom[datapointaMetadata[key]] === undefined) context__metadataFilters[key].linkedAtom[datapointaMetadata[key]] = { datapoint_ids: [] }

            context__metadataFilters[key].options[datapointaMetadata[key]] = {
                id: datapointaMetadata[key],
                visible: true,
                color: "#333333",
            }
            context__metadataFilters[key].linkedAtom[datapointaMetadata[key]] = {
                id: datapointaMetadata[key],
                name: datapointaMetadata[key],
                datapoint_ids: [...context__metadataFilters[key].linkedAtom[datapointaMetadata[key]].datapoint_ids, datapoint.id]
            }
        })

        // set up our datapoint object
        context__datapointsObject[datapoint.id] = {
            ...datapoint,
            tag_ids: [],
            inferences: [],
            annotations: [],
            metadata: datapointaMetadata,
            object_datapoint_ids: []
        }

        // add our datapoint to its dataset
        context__datasetsObject[datapoint.dataset_id].datapoint_ids.push(datapoint.id)
    })

    // add our cross link from tags back onto datapoints
    Object.values(context__tagsObject).map(function (tag) {
        tag.datapoint_ids.map(dpid => {
            context__datapointsObject[dpid].tag_ids.push(tag.id)
        })
    })

    // take datapoint annotation data and add it to the category datapoint list
    let annsIdsToAdd = {}
    labels.forEach((label) => {
        const labelData = JSON.parse(label.data)
        context__labelsObject[label.id] = {
            ...label,
            data: labelData
        }

        context__datapointsObject[label.datapoint_id].annotations = labelData.annotations

        labelData.annotations.forEach((annotation) => {
            const categoryId = annotation.category_id
            if (annsIdsToAdd[categoryId] === undefined) annsIdsToAdd[categoryId] = new Set()
            annsIdsToAdd[categoryId].add(label.datapoint_id)
        })
    })

    // load up our datapoint ids into categories
    Object.keys(annsIdsToAdd).map((c) => {
        const dps = (annsIdsToAdd[c]).keys()
        context__categoriesObject[c].datapoint_ids = [...dps]
    })

    // create resources object
    resources.forEach((resource) => {
        context__resourcesObject[resource.id] = {
            ...resource
        }
    })

    // load inferences object
    let context_inference_categories = {}
    inferences.forEach((inference) => {
        const inferenceData = JSON.parse(inference.data)
        context__inferencesObject[inference.id] = {
            ...inference,
            data: inferenceData
        }

        if (inferenceData.annotations) context__datapointsObject[inference.datapoint_id].inferences = inferenceData.annotations

        inferenceData.annotations.forEach((annotation) => {
            const categoryId = annotation.category_id
            if (context_inference_categories[categoryId] === undefined) context_inference_categories[categoryId] = new Set()
            context_inference_categories[categoryId].add(inference.datapoint_id)
        })
    })

    // take datapoint annotation data and add it to the category datapoint list
    // load up our datapoint ids into categories
    Object.keys(context_inference_categories).map((c) => {
        const dps = (context_inference_categories[c]).keys()
        context__inferenceCategoriesObject[c].datapoint_ids = [...dps]
    })

    // Specifically synthetically create our object datapoints
    // they don't 'exist' in the backend as such right now... but we want to use the rest of the UI components

    // create the object version of things for inferences
    // has to come after datapoints object and its annotations have been filled
    var i = 0
    var j = 0
    var targetDatapointIdMap = {}
    let object_inference_categories = {}
    Object.values(context__datapointsObject).map(dp => {
        dp.inferences.map(ann => {
            let hasBoundingBoxes = (ann.bbox !== undefined)
            if (hasBoundingBoxes) {

                // create the resource
                object__resourcesObject[j] = {
                    id: j,
                    uri: context__resourcesObject[dp.id].uri
                }

                targetDatapointIdMap[ann.id] = i

                var assoc_label = []
                if (ann.label_id !== "") assoc_label = [dp.annotations.find(d => d.id == ann.label_id)]

                // create the points
                // we store the single inferences object in inferences
                // if there is a corresponding label, we store it in annotations
                object__datapointsObject[i] = {
                    inferences: [ann],
                    annotations: assoc_label,
                    dataset_id: dp.dataset_id,
                    id: i,
                    metadata: {},
                    tag_ids: [],
                    source_datapoint_id: dp.id,
                    resource_id: j,
                    inference: true
                }
                dp.object_datapoint_ids.push(i)

                // add itself to the dataset
                object__datasetsObject[dp.dataset_id].datapoint_ids.push(i)

                if (assoc_label.length != 0) {
                    const categoryId = assoc_label[0].category_id
                    if (object_inference_categories[categoryId] === undefined) object_inference_categories[categoryId] = new Set()
                    object_inference_categories[categoryId].add(i)
                }

                i++
                j++
            }

        })

    })

    // take datapoint annotation data and add it to the category datapoint list
    // load up our datapoint ids into categories
    Object.keys(object_inference_categories).map((c) => {
        const dps = (object_inference_categories[c]).keys()
        object__labelCategoriesObject[c].datapoint_ids = [...dps]
    })

    // load tags object and fill in the object datapoints that have that tag 
    tags.forEach((tag) => {
        var datapointList

        // filter out cases where the tagdatapoint is actually on the label annotation and not the datapoint
        if ((tag.target !== null)) {
            var objectDatapointId = targetDatapointIdMap[tag.target]

            if (object__tagsObject[tag.tag.id]?.datapoint_ids === undefined) {
                datapointList = [objectDatapointId]
            } else {
                datapointList = [...object__tagsObject[tag.tag.id].datapoint_ids, objectDatapointId]
            }

            object__tagsObject[tag.tag.id] = {
                ...tag.tag,
                datapoint_ids: datapointList
            }

        }
    })
    // add our cross link from tags back onto datapoints
    Object.values(object__tagsObject).map(function (tag) {
        tag.datapoint_ids.map(dpid => {
            object__datapointsObject[dpid].tag_ids.push(tag.id)
        })
    })

    // create our filters based on the metadata set on the inference annotation
    let annsInferenceIdsToAdd = {}
    Object.values(object__datapointsObject).map(inferenceDp => {
        let categoryId = inferenceDp.inferences[0].category_id
        if (annsInferenceIdsToAdd[categoryId] === undefined) annsInferenceIdsToAdd[categoryId] = new Set()
        annsInferenceIdsToAdd[categoryId].add(inferenceDp.id)

        inferenceDp.inferences.map(ann => {
            Object.keys(ann.metadata).map(key => {
                if (object__metadataFiltersObject[key] === undefined) object__metadataFiltersObject[key] = { name: key, options: {}, type: 0, linkedAtom: {} }
                if (object__metadataFiltersObject[key].linkedAtom[ann.metadata[key]] === undefined) object__metadataFiltersObject[key].linkedAtom[ann.metadata[key]] = { datapoint_ids: [] }

                object__metadataFiltersObject[key].options[ann.metadata[key]] = {
                    id: ann.metadata[key],
                    visible: true,
                    color: "#333333",
                }
                object__metadataFiltersObject[key].linkedAtom[ann.metadata[key]] = {
                    id: ann.metadata[key],
                    name: ann.metadata[key],
                    datapoint_ids: [...object__metadataFiltersObject[key].linkedAtom[ann.metadata[key]].datapoint_ids, inferenceDp.id]
                }
            })
        })
    })

    Object.keys(annsInferenceIdsToAdd).map((c) => {
        const dps = (annsInferenceIdsToAdd[c]).keys()
        object__inferenceCategoriesObject[c].datapoint_ids = [...dps]
    })

    // if we dont have any bounding boxes... wipe out the other data structures
    if (Object.values(object__datapointsObject).length === 0) {
        object__inferenceCategoriesObject = {}
        object__resourcesObject = {}
        object__datasetsObject = {}
    }

    self.postMessage({
        numberOfDatapoints: datapoints.length,

        // datapoints stuff
        context__datapoints: context__datapointsObject,
        context__datasets: context__datasetsObject,
        context__labels: context__labelsObject,
        context__inferences: context__inferencesObject,
        context__resources: context__resourcesObject,
        context__tags: context__tagsObject,
        context__categories: context__categoriesObject,
        context__metadataFilters: context__metadataFilters,

        context__inferenceCategories: context__inferenceCategoriesObject,

        // object__s stuff
        object__datapoints: object__datapointsObject,
        object__datasets: object__datasetsObject,
        object__labels: object__labelsObject, // just an empty object right now, only here for consistency
        object__resources: object__resourcesObject,
        object__tags: object__tagsObject,
        object__metadataFilters: object__metadataFiltersObject,

        object__labelCategories: object__labelCategoriesObject,
        object__inferenceCategories: object__inferenceCategoriesObject,
    })
}