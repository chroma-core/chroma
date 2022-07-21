// @ts-nocheck
import chroma from 'chroma-js'
import distinctColors from 'distinct-colors'
import { cocoDetection } from './cocodetection'

// our datapoints are 1 index
// but our datastructures are 0 indexed
// this function only exists to aid in sanity
// eg datapoint 855 in the database, is 854 in our local arrays
export const datapointIndexToPointIndex = (id: number) => {
  return (id - 1)
}
export const pointIndexToDataPointIndex = (id: number) => {
  return (id + 1)
}

export const getMostRecentCreatedAt = function (data: any) {
  // console.log('getMostRecentCreatedAt', data)
  if (data.length == 0) return undefined
  return data.reduce((p1: any, p2: any) => {
    return new Date(p1.createdAt) > new Date(p2.createdAt) ? p1 : p2;
  });
}

// converts string JSON coming back from a REST endpoint to JSON
export const jsonifyDatapoints = function (datapoints: any) {
  datapoints.map((datapoint: any) => {
    // Metadata may not be present 
    if (datapoint.metadata_) {
      datapoint.metadata_ = JSON.parse(datapoint.metadata_)
    } else {
      datapoint.metadata_ = ""
    }
    datapoint.label.data = JSON.parse(datapoint.label.data)

    // all datapoints should have inference
    if (datapoint.inference === null) datapoint.inference = { data: undefined }
    datapoint.inference.data = cocoDetection//JSON.parse(datapoint.inference?.data)

    // add other state we will want to track
    datapoint.visible = true
  })
  return datapoints
}

let FILTERS = [
  {
    name: 'Inferences',
    type: 'discrete',
    fetchFn: function (datapoint) {
      return datapoint.inference.data.categories.map(category => category.name)
    },
    removeDupes(filterOptions) {
      return filterOptions.filter((v, i, a) => a.findIndex(v2 => (v2.name === v.name)) === i)
    },
    defaultSort(filterOptions) {
      return filterOptions.sort(function (a, b) { return a.name - b.name; });
    },
    optionsSet: [],
    filterBy: function (evalFields, optionsSet) {
      let visible = true;
      evalFields.map(evalField => {
        visible = ((visible == true) ? optionsSet.find(o => o.name === evalField).visible : visible) // if visible is true, potentially set it to false, else keep it false
      })
      return visible
    },
    colorBy: function () { },
    generateColors: function (numColors) {
      return distinctColors({
        "count": numColors, //filter.optionsSet.length,
        "lightMin": 20,
        "lightMax": 80,
        "chromaMin": 80
      }).map(color => color.hex())
    }
  },
  {
    name: 'Labels',
    type: 'discrete',
    fetchFn: function (datapoint) {
      return datapoint.label.data.annotations.map(a => a.category_id)
    },
    removeDupes(filterOptions) {
      return filterOptions.filter((v, i, a) => a.findIndex(v2 => (v2.name === v.name)) === i)
    },
    defaultSort(filterOptions) {
      return filterOptions.sort(function (a, b) { return a.name - b.name; });
    },
    optionsSet: [],
    filterBy: function (evalFields, optionsSet) {
      let visible = true;
      evalFields.map(evalField => {
        visible = ((visible == true) ? optionsSet.find(o => o.name === evalField).visible : visible) // if visible is true, potentially set it to false, else keep it false
      })
      return visible
    },
    colorBy: function () { },
    generateColors: function (numColors) {
      return distinctColors({
        "count": numColors, //filter.optionsSet.length,
        "lightMin": 20,
        "lightMax": 80,
        "chromaMin": 80
      }).map(color => color.hex())
    }
  },
  {
    name: 'Quality',
    type: 'continuous',
    fetchFn: function (datapoint) {
      return [Math.exp(-parseFloat(datapoint.metadata_.distance_score)) * 100]
    },
    removeDupes(filterOptions) {
      return filterOptions
    },
    defaultSort(filterOptions) {
      filterOptions.maxVisible = filterOptions.max
      filterOptions.minVisible = filterOptions.min
      return filterOptions
    },
    optionsSet: {
      min: Infinity,
      max: -Infinity,
      minVisible: 0,
      maxVisible: 0
    },
    filterBy: function (quality, optionsSet) {
      let visible = true;
      quality = quality[0] // just a singular value
      if ((quality >= optionsSet.maxVisible) || (quality <= optionsSet.minVisible)) {
        visible = false;
      }
      return visible
    },
    colorBy: function () { },
  },
  {
    name: 'Tags',
    type: 'discrete',
    fetchFn: function (datapoint) {
      return datapoint.tags.map(tag => tag.tag.name)
    },
    removeDupes(filterOptions) {
      return filterOptions.filter((v, i, a) => a.findIndex(v2 => (v2.name === v.name)) === i)
    },
    defaultSort(filterOptions) {
      return filterOptions.sort(function (a, b) { return a.name - b.name; });
    },
    optionsSet: [],
    filterBy: function (evalFields, optionsSet) {
      let visible = true;
      evalFields.map(evalField => {
        visible = ((visible == true) ? optionsSet.find(o => o.name === evalField).visible : visible) // if visible is true, potentially set it to false, else keep it false
      })
      return visible
    },
    colorBy: function () { },
    generateColors: function (numColors) {
      return distinctColors({
        "count": numColors, //filter.optionsSet.length,
        "lightMin": 20,
        "lightMax": 80,
        "chromaMin": 80
      }).map(color => color.hex())
    }
  },
  {
    name: 'Datasets',
    type: 'discrete',
    fetchFn: function (datapoint) {
      return [datapoint.dataset.name]
    },
    removeDupes(filterOptions) {
      return filterOptions.filter((v, i, a) => a.findIndex(v2 => (v2.name === v.name)) === i)
    },
    defaultSort(filterOptions) {
      return filterOptions.sort(function (a, b) { return a.name - b.name; });
    },
    optionsSet: [],
    filterBy: function (evalFields, optionsSet) {
      let visible;
      evalFields.map(evalField => {
        if (visible !== false) {
          var filterVisible = optionsSet.find(o => o.name === evalField).visible
          visible = filterVisible
        }
      })
      return visible
    },
    colorBy: function () { },
    generateColors: function (numColors) {
      return distinctColors({
        "count": numColors, //filter.optionsSet.length,
        "lightMin": 20,
        "lightMax": 80,
        "chromaMin": 80
      }).map(color => color.hex())
    }
  },
  {
    name: 'Label/Inference Match',
    type: 'discrete',
    fetchFn: function (datapoint) {
      return [datapoint.labelInferenceMatch]
    },
    removeDupes(filterOptions) {
      return filterOptions.filter((v, i, a) => a.findIndex(v2 => (v2.name === v.name)) === i)
    },
    defaultSort(filterOptions) {
      return filterOptions.sort(function (a, b) { return a.name - b.name; });
    },
    optionsSet: [],
    filterBy: function (evalFields, optionsSet) {
      let visible;
      evalFields.map(evalField => {
        if (visible !== false) {
          var filterVisible = optionsSet.find(o => o.name === evalField).visible
          visible = filterVisible
        }
      })
      return visible
    },
    colorBy: function () { },
    generateColors: function (numColors) {
      return ["#27ce47", "#ce2731", "#999999"]
    }
  },
]

export const buildFilters = (datapoints: any) => {
  // get all available options for the various properties
  let categories = []

  datapoints.map((datapoint: any) => {

    // preprocess, mainly to add fields we don't already have
    // hard-coded for now, in the future filters could publish to this
    const labelClass = datapoint.label?.data.categories[0].name
    const inferenceClass = datapoint.inference?.data.categories[0].name
    if (labelClass && inferenceClass && (labelClass === inferenceClass)) {
      datapoint.labelInferenceMatch = 'Agree'
    } else if (labelClass && inferenceClass && (labelClass !== inferenceClass)) {
      datapoint.labelInferenceMatch = 'Disagree'
    } else {
      datapoint.labelInferenceMatch = 'Not enough data'
    }

    datapoint.label.data.categories.map(category => {
      categories.push(category)
    })

    FILTERS.map(filter => {
      const newOptions = filter.fetchFn(datapoint)

      if (filter.type == 'discrete') {
        newOptions.map(newOption => {
          filter.optionsSet!.push({
            name: newOption,
            visible: true,
            color: "#333333"
          })
        })

      } else if (filter.type == 'continuous') {
        newOptions.map(newOption => {
          let num = parseFloat(newOption) // make sure for continuous we are forcing a number
          filter.optionsSet!.min! = (filter.optionsSet?.min! > num) ? num : filter.optionsSet!.min
          filter.optionsSet!.max! = (filter.optionsSet?.max! < num) ? num : filter.optionsSet!.max
        })
      }

    })
  })

  // remove dupes and sort lexographically
  FILTERS.map(filter => {
    filter.optionsSet = filter.defaultSort(filter.removeDupes(filter.optionsSet))
  })

  // add color options
  FILTERS.map(filter => {
    if (filter.type == 'discrete') {
      let colorsOpts = filter.generateColors(filter.optionsSet.length)
      filter.optionsSet.map((option, index) => {
        option.color = colorsOpts[index]
      })
    }
    if (filter.type == 'continuous') {
      var colorScale = chroma.scale(["5B68A8", "5CC8C6", "87DF9C", "E4ED58", "F8EB49", "FACE31", "F79A17", "DE500F"]).colors(50)
      filter.optionsSet.colors = colorScale
    }
  })
  
  categories = categories.filter((v, i, a) => a.findIndex(v2 => (v2.id === v.id)) === i)
  console.log('categories', categories)

  return FILTERS
}

export const applyAllFilters = (datapoints: any, filters: any) => {
  datapoints.map((datapoint: any) => {
    datapoint.visible = true

    // of of these filters may set visible to false
    for (let i = 0; i < FILTERS.length; i++) {
      const filter = FILTERS[i];
      applyFilter(datapoint, filter)
      if (datapoint.visible == false) break; // if any filter hides this, stop evaluating them
    }
  })

  return datapoints
}

const applyFilter = (datapoint: any, filter: any) => {
  const newOptions = filter.fetchFn(datapoint)
  datapoint.visible = filter.filterBy(newOptions, filter.optionsSet)
}

export const insertProjectionsOntoDatapoints = (datapoints: any, projections: any) => {
  projections.map(projection => {
    const datapointId = projection.embedding.datapoint_id
    let datapoint = datapoints.find(dp => dp.id == datapointId)
    datapoint.projection = projection
  })

  return datapoints
}