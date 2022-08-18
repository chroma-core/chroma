import { useAtom } from 'jotai'
import React, { useCallback, useEffect } from 'react'
import { datapointsAtom, labelsAtom, tagsAtom, resourcesAtom, inferencesAtom, datasetsAtom, categoriesAtom, projectionsAtom, inferenceFilterAtom, categoryFilterAtom, tagFilterAtom, datasetFilterAtom, visibleDatapointsAtom, metadataFiltersAtom, labelVisibleDatapointsAtom, labelMetadataFiltersAtom, labelDatapointsAtom, labelCategoryFilterAtom, labelCategoriesAtom, labelDatasetsAtom, labelDatasetFilterAtom, globalCategoryFilterAtom, globalDatasetFilterAtom, globalVisibleDatapointsAtom } from './atoms'
import { FilterOption, Filter, FilterType, Datapoint } from './types'

import chroma from 'chroma-js'
import distinctColors from 'distinct-colors'

const Updater: React.FC = () => {
  // Atoms
  const [datapoints, updatedatapoints] = useAtom(datapointsAtom)
  const [labeldatapoints, updatelabeldatapoints] = useAtom(labelDatapointsAtom)
  const [labels, updatelabels] = useAtom(labelsAtom)
  const [tags, updatetags] = useAtom(tagsAtom)
  const [resources, updateresources] = useAtom(resourcesAtom)
  const [inferences, updateinferences] = useAtom(inferencesAtom)
  const [datasets, updatedatasets] = useAtom(datasetsAtom)
  const [labeldatasets, updatelabeldatasets] = useAtom(labelDatasetsAtom)
  const [categories, updatecategories] = useAtom(categoriesAtom)
  const [labelcategories, updatelabelcategories] = useAtom(labelCategoriesAtom)
  const [projections, updateprojections] = useAtom(projectionsAtom)
  const [visibleDatapoints, updatevisibleDatapoints] = useAtom(visibleDatapointsAtom)
  const [labelvisibleDatapoints, updatelabelvisibleDatapoints] = useAtom(labelVisibleDatapointsAtom)

  // Filter Atoms
  // const [inferenceFilter, updateinferenceFilter] = useAtom(inferenceFilterAtom)
  const [categoryFilter, updatecategoryFilter] = useAtom(categoryFilterAtom)
  const [labelcategoryFilter, updatelabelcategoryFilter] = useAtom(labelCategoryFilterAtom)
  const [datasetFilter, updatedatasetFilter] = useAtom(datasetFilterAtom)
  const [labeldatasetFilter, updatelabeldatasetFilter] = useAtom(labelDatasetFilterAtom)
  const [tagFilter, updatetagFilter] = useAtom(tagFilterAtom)
  const [metadataFilters, updateMetadataFilter] = useAtom(metadataFiltersAtom)

  // whenever a filter is changed... generate the list of datapoints ids to hide
  const filtersToWatch = [categoryFilter, datasetFilter, tagFilter]
  const filtersToObserve = [categoryFilter, datasetFilter, tagFilter, ...Object.values(metadataFilters)]
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
  }, [...filtersToWatch, metadataFilters])

  // // whenever a filter is changed... generate the list of datapoints ids to hide
  const labelfiltersToObserve = [labelcategoryFilter, labeldatasetFilter]
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
  }, [labelcategoryFilter, labeldatasetFilter])

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
      name: 'Category',
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
      name: 'Category',
      type: FilterType.Discrete,
      options: options,
      linkedAtom: labelcategories
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
      linkedAtom: datasets
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
      linkedAtom: labeldatasets
    }
    updatelabeldatasetFilter(newDatasetFilter)
  }, [labeldatasets])

  return null
}

export default Updater
