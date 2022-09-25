import { useAtom } from 'jotai'
import React, { useCallback, useEffect } from 'react'
import {
  context__datapointsAtom, context__labelsAtom, context__tagsAtom, context__resourcesAtom, context__inferencesAtom, context__datasetsAtom, context__categoriesAtom, context__projectionsAtom, context__inferenceFilterAtom, context__categoryFilterAtom, context__tagFilterAtom, context__datasetFilterAtom, visibleDatapointsAtom, context__metadataFiltersAtom, labelVisibleDatapointsAtom,
  object__metadataFiltersAtom, object__datapointsAtom, object__categoryFilterAtom, object__categoriesAtom, object__datasetsAtom, object__datasetFilterAtom, globalCategoryFilterAtom, globalDatasetFilterAtom, globalVisibleDatapointsAtom, object__tagFilterAtom, object__tagsAtom, context__inferencecategoriesAtom, context__inferenceCategoryFilterAtom
} from './atoms'
import { FilterOption, Filter, FilterType, Datapoint } from './types'

import chroma from 'chroma-js'
import distinctColors from 'distinct-colors'

const Updater: React.FC = () => {
  // Atoms
  const [datapoints, updatedatapoints] = useAtom(context__datapointsAtom)
  const [labeldatapoints, updatelabeldatapoints] = useAtom(object__datapointsAtom)

  const [labels, updatelabels] = useAtom(context__labelsAtom)

  const [tags, updatetags] = useAtom(context__tagsAtom)
  const [object__tags, updateobjecttags] = useAtom(object__tagsAtom)

  const [resources, updateresources] = useAtom(context__resourcesAtom)

  const [inferences, updateinferences] = useAtom(context__inferencesAtom)

  const [datasets, updatedatasets] = useAtom(context__datasetsAtom)
  const [labeldatasets, updatelabeldatasets] = useAtom(object__datasetsAtom)

  const [categories, updatecategories] = useAtom(context__categoriesAtom)
  const [labelcategories, updatelabelcategories] = useAtom(object__categoriesAtom)
  const [inferencecategories, updateinferencecategories] = useAtom(context__inferencecategoriesAtom)

  const [projections, updateprojections] = useAtom(context__projectionsAtom)
  const [visibleDatapoints, updatevisibleDatapoints] = useAtom(visibleDatapointsAtom)
  const [labelvisibleDatapoints, updatelabelvisibleDatapoints] = useAtom(labelVisibleDatapointsAtom)

  // Filter Atoms
  // const [inferenceFilter, updateinferenceFilter] = useAtom(inferenceFilterAtom)
  const [categoryFilter, updatecategoryFilter] = useAtom(context__categoryFilterAtom)
  const [labelcategoryFilter, updatelabelcategoryFilter] = useAtom(object__categoryFilterAtom)
  const [datasetFilter, updatedatasetFilter] = useAtom(context__datasetFilterAtom)
  const [labeldatasetFilter, updatelabeldatasetFilter] = useAtom(object__datasetFilterAtom)
  const [inferenceCategoryFilter, updateInferenceCategoryFilter] = useAtom(context__inferenceCategoryFilterAtom)

  const [tagFilter, updatetagFilter] = useAtom(context__tagFilterAtom)
  const [object__tagFilter, updateobjecttagFilter] = useAtom(object__tagFilterAtom)

  const [context__metadataFilters, updateMetadataFilter] = useAtom(context__metadataFiltersAtom)
  const [object__metadataFilters, updatelabelMetadataFilter] = useAtom(object__metadataFiltersAtom)

  // whenever a filter is changed... generate the list of datapoints ids to hide
  const filtersToWatch = [categoryFilter, datasetFilter, tagFilter, inferenceCategoryFilter]
  const filtersToObserve = [categoryFilter, datasetFilter, tagFilter, inferenceCategoryFilter, ...Object.values(context__metadataFilters)]
  useEffect(() => {
    let visibleDps: number[] = []
    let datapointsToHide: number[] = []
    Object.values(datapoints).map(function (val, keyIndex) {
      let dp = val
      visibleDps.push(dp.id)

      for (let i = 0; i < filtersToObserve.length; i++) {
        let filter = filtersToObserve[i]
        for (let j = 0; j < filter!.options!.length; j++) {
          var result = filter!.options![j].evalDatapoint(dp, filter!.options![j], filter)
          if (result) {
            datapointsToHide.push(dp.id)
            i = filtersToObserve.length
            j = filter!.options!.length // break out of both loops
          }
        }
      }
    })
    visibleDps = visibleDps.filter((el) => !datapointsToHide.includes(el));
    updatevisibleDatapoints(visibleDps)
  }, [...filtersToWatch, context__metadataFilters])

  // // whenever a filter is changed... generate the list of datapoints ids to hide
  const labelfiltersToObserve = [labelcategoryFilter, labeldatasetFilter, object__tagFilter, ...Object.values(object__metadataFilters)]
  useEffect(() => {
    let visibleDps: number[] = []
    let datapointsToHide: number[] = []
    Object.values(labeldatapoints).map(function (val, keyIndex) {
      let dp = val
      visibleDps.push(dp.id)

      for (let i = 0; i < labelfiltersToObserve.length; i++) {
        let filter = labelfiltersToObserve[i]
        for (let j = 0; j < filter!.options!.length; j++) {
          // @ts-ignore
          var result = filter!.options![j].evalDatapoint(dp, filter!.options![j], filter)
          if (result) {
            datapointsToHide.push(dp.id)
            i = labelfiltersToObserve.length
            j = filter!.options!.length // break out of both loops
          }
        }
      }
    })
    visibleDps = visibleDps.filter((el) => !datapointsToHide.includes(el));
    updatelabelvisibleDatapoints(visibleDps)
  }, [labelcategoryFilter, labeldatasetFilter, object__metadataFilters, object__tagFilter])

  // categories filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(categories).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(categories).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          const match = datapoint.annotations.findIndex(a => a.category_id == option.id)
          if ((option.visible == false) && (match > -1)) return true
          else return false
        }
      }
      return option
    })

    let newCategoryFilter: Filter = {
      name: 'Label Category',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: categories,
      fetchFn: (datapoint) => {
        return datapoint.annotations[0].category_id
      }
    }
    updatecategoryFilter(newCategoryFilter)
  }, [categories])

  // inferencecategories filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(inferencecategories).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(inferencecategories).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          const match = datapoint.inferences.findIndex(a => a.category_id == option.id)
          if ((option.visible == false) && (match > -1)) return true
          else return false
        }
      }
      return option
    })

    let newCategoryFilter: Filter = {
      name: 'Inference Category',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: inferencecategories,
      fetchFn: (datapoint) => {
        return datapoint.inferences[0].category_id
      }
    }
    updateInferenceCategoryFilter(newCategoryFilter)
  }, [inferencecategories])

  // categories filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(categories).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(categories).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          const match = datapoint.annotations.findIndex(a => a.category_id == option.id)
          if ((option.visible == false) && (match > -1)) return true
          else return false
        }
      }
      return option
    })

    let newCategoryFilter: Filter = {
      name: 'Label Category',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: categories,
      fetchFn: (datapoint) => {
        return datapoint.annotations[0].category_id
      }
    }
    updatecategoryFilter(newCategoryFilter)
  }, [categories])

  // labelcategories filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(labelcategories).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(labelcategories).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          const match = datapoint.annotations.findIndex(a => a.category_id == option.id)
          if ((option.visible == false) && (match > -1)) return true
          else return false
        }
      }
      return option
    })

    let newCategoryFilter: Filter = {
      name: 'Label Category',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: labelcategories,
      fetchFn: (datapoint) => {
        return datapoint.annotations[0].category_id
      }
    }
    updatelabelcategoryFilter(newCategoryFilter)
  }, [labelcategories])

  // tags filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(tags).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(tags).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          if ((option.visible == false) && (datapoint.tag_ids.includes(option.id))) return true
          else return false
        }
      }
      return option
    })

    let newTagFilter: Filter = {
      name: 'Tags',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: tags,
    }
    updatetagFilter(newTagFilter)
  }, [tags])

  // object tags filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(object__tags).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(object__tags).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          if ((option.visible == false) && (datapoint.tag_ids.includes(option.id))) return true
          else return false
        }
      }
      return option
    })

    let newTagFilter: Filter = {
      name: 'Tags',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: object__tags,
    }
    updateobjecttagFilter(newTagFilter)
  }, [object__tags])

  // dataset filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(datasets).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(datasets).map((c, i) => {
      let option: FilterOption = {
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          if ((option.visible == false) && (datapoint.dataset_id == option.id)) return true
          else return false
        }
      }
      return option
    })

    let newDatasetFilter: Filter = {
      name: 'Datasets',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: datasets,
      fetchFn: (datapoint) => {
        return datapoint.dataset_id
      }
    }
    updatedatasetFilter(newDatasetFilter)
  }, [datasets])

  // dataset filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.values(labeldatasets).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.values(labeldatasets).map((c, i) => {
      let option: FilterOption = {
        id: c.id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          if ((option.visible == false) && (datapoint.dataset_id == option.id)) return true
          else return false
        }
      }
      return option
    })

    let newDatasetFilter: Filter = {
      name: 'Datasets',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: labeldatasets,
      fetchFn: (datapoint) => {
        return datapoint.dataset_id
      }
    }
    updatelabeldatasetFilter(newDatasetFilter)
  }, [labeldatasets])

  return null
}

export default Updater
