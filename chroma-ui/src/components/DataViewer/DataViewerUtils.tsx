// @ts-nocheck
import distinctColors from 'distinct-colors'

export const getMostRecentCreatedAt = function (data: any) {
  return data.reduce((p1: any, p2: any) => {
    return new Date(p1.createdAt) > new Date(p2.createdAt) ? p1 : p2;
  });
}

// converts string JSON coming back from a REST endpoint to JSON
export const jsonifyDatapoints = function (datapoints: any) {
  datapoints.map((datapoint: any) => {
    datapoint.metadata_ = JSON.parse(datapoint.metadata_)
    datapoint.label.data = JSON.parse(datapoint.label.data)

    // add other state we will want to track
    datapoint.visible = true
  })
  return datapoints
}

let FILTERS = [
  {
    name: 'Classes',
    type: 'discrete',
    fetchFn: function (datapoint) {
      return datapoint.label.data.categories.map(category => category.name)
    },
    removeDupes(filterOptions) {
      return filterOptions.filter((v, i, a) => a.findIndex(v2 => (v2.name === v.name)) === i)
    },
    defaultSort(filterOptions) {
      return filterOptions.sort(function (a, b) { return a.name - b.name; });
    },
    optionsSet: [],
    sortBy: function () { },
    filterBy: function (evalFields, optionsSet) {
      let visible = true;
      evalFields.map(evalField => {
        visible = ((visible == true) ? optionsSet.find(o => o.name === evalField).visible : visible) // if visible is true, potentially set it to false, else keep it false
      })
      return visible
    },
    colorBy: function () { },
  },
  // {
  //   name: 'Quality',
  //   type: 'continuous',
  //   fetchFn: function (datapoint) {
  //     return [datapoint.metadata_.quality]
  //   },
  //   removeDupes(filterOptions) {
  //     return filterOptions
  //   },
  //   defaultSort(filterOptions) {
  //     filterOptions.maxVisible = filterOptions.max
  //     filterOptions.minVisible = filterOptions.min
  //     return filterOptions
  //   },
  //   optionsSet: {
  //     min: Infinity,
  //     max: -Infinity,
  //     minVisible: 0,
  //     maxVisible: 0
  //   },
  //   sortBy: function () { },
  //   filterBy: function (quality, optionsSet) {
  //     let visible = true;
  //     if ((quality <= optionsSet.maxVisible) && (quality >= optionsSet.minVisible)) {
  //       visible = false;
  //     }

  //     return true
  //   },
  //   colorBy: function () { },
  // },
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
    sortBy: function () { },
    filterBy: function (evalFields, optionsSet) {
      let visible = true;
      evalFields.map(evalField => {
        visible = ((visible == true) ? optionsSet.find(o => o.name === evalField).visible : visible) // if visible is true, potentially set it to false, else keep it false
      })
      return visible
    },
    colorBy: function () { },
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
    sortBy: function () { },
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
  },
]

export const buildFilters = (datapoints: any) => {
  datapoints.map((datapoint: any) => {
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
          filter.optionsSet!.min! = (filter.optionsSet?.min! > newOption) ? newOption : filter.optionsSet!.min
          filter.optionsSet!.max! = (filter.optionsSet?.max! < newOption) ? newOption : filter.optionsSet!.max
        })
      }

    })
  })

  FILTERS.map(filter => {
    filter.optionsSet = filter.defaultSort(filter.removeDupes(filter.optionsSet))
  })

  FILTERS.map(filter => {
    if (filter.type == 'discrete') {
      let colorsOpts = distinctColors({
        "count": filter.optionsSet.length,
        "lightMin": 20,
        "lightMax": 80,
        "chromaMin": 80
      })
      console.log('colorsOpts', colorsOpts, filter)
      filter.optionsSet.map((option, index) => {
        option.color = colorsOpts[index].hex()
      })
    }

  })

  return FILTERS
}

export const applyAllFilters = (datapoints: any, filters: any) => {
  datapoints.map((datapoint: any) => {
    // so maybe I do need to eval every filter..... 
    // after i complete this... then i will need to figure out which projections i should should based on their embedding.datapoint.id

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

// const applySingleFilter = (datapoints: any, filter: any) => {
//     datapoints.map((datapoint: any) => {
//         applyFilter(datapoint, filter)
//     })
// }

const applyFilter = (datapoint: any, filter: any) => {
  const newOptions = filter.fetchFn(datapoint)
  datapoint.visible = filter.filterBy(newOptions, filter.optionsSet)
}

// basically what i will want to do is........ (this is for filter by)
// 1. iterate through list of datapoints
// 2. if a filter "applies" to that datapoint, then mark it as not visible
// 3. pass the visible datapoints to the right sidebar data viewer
// 4. use the datapoint ids that are visible to find the corresponding projections and show/hide those

// then you do color by and sort by seperately later


export const insertProjectionsOntoDatapoints = (datapoints: any, projections: any) => {
  projections.map(projection => {
    const datapointId = projection.embedding.datapoint_id
    let datapoint = datapoints.find(dp => dp.id == datapointId)
    datapoint.projection = projection // the second one is where im going to inject my coloration options
  })

  return datapoints
}