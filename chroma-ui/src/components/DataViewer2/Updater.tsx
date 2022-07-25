import { useAtom } from 'jotai'
import React, { useCallback, useEffect } from 'react'
import { datapointsAtom, labelsAtom, tagsAtom, resourcesAtom, inferencesAtom, datasetsAtom, categoriesAtom, projectionsAtom, inferenceFilterAtom, categoryFilterAtom, tagFilterAtom, datasetFilterAtom, visibleDatapointsAtom } from './atoms'
import { FilterOption, Filter, FilterType, Datapoint } from './types'

import chroma from 'chroma-js'
import distinctColors from 'distinct-colors'

const Updater: React.FC = () => {
  // Atoms
  const [datapoints, updatedatapoints] = useAtom(datapointsAtom)
  const [labels, updatelabels] = useAtom(labelsAtom)
  const [tags, updatetags] = useAtom(tagsAtom)
  const [resources, updateresources] = useAtom(resourcesAtom)
  const [inferences, updateinferences] = useAtom(inferencesAtom)
  const [datasets, updatedatasets] = useAtom(datasetsAtom)
  const [categories, updatecategories] = useAtom(categoriesAtom)
  const [projections, updateprojections] = useAtom(projectionsAtom)
  const [visibleDatapoints, updatevisibleDatapoints] = useAtom(visibleDatapointsAtom)

  // Filter Atoms
  // const [inferenceFilter, updateinferenceFilter] = useAtom(inferenceFilterAtom)
  const [categoryFilter, updatecategoryFilter] = useAtom(categoryFilterAtom)
  const [datasetFilter, updatedatasetFilter] = useAtom(datasetFilterAtom)
  const [tagFilter, updatetagFilter] = useAtom(tagFilterAtom)

  // whenever a filter is changed... generate the list of datapoints ids to hide
  const filtersToObserve = [categoryFilter, datasetFilter, tagFilter]
  useEffect(() => {
    let visibleDps: number[] = []
    let datapointsToHide: number[] = []
    Object.keys(datapoints).map(function (keyName, keyIndex) {
      let dp = datapoints[parseInt(keyName, 10)]
      visibleDps.push(dp.id)

      for (let i = 0; i < filtersToObserve.length; i++) {
        let filter = filtersToObserve[i]
        for (let j = 0; j < filter!.options!.length; j++) {
          var result = filter!.options![j].evalDatapoint(dp, filter!.options![j])
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
  }, filtersToObserve)

  // categories filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.keys(categories).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.keys(categories).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: categories[c].id,
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
      linkedAtom: categories
    }
    updatecategoryFilter(newCategoryFilter)
  }, [categories])

  // tags filter
  useEffect(() => {
    var colors = distinctColors({
      "count": Object.keys(tags).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.keys(tags).map((c, i) => {
      let option: FilterOption = {
        // @ts-ignore
        id: tags[c].id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          if ((option.visible == false) && (datapoint.tags.includes(option.id))) return true
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
      "count": Object.keys(datasets).length,
      "lightMin": 20,
      "lightMax": 85,
      "chromaMin": 50
    }).map(color => color.hex())

    let options: FilterOption[] = Object.keys(datasets).map((c, i) => {
      let option: FilterOption = {
        id: datasets[parseInt(c, 10)].id,
        visible: true,
        color: colors[i],
        evalDatapoint: (datapoint: Datapoint, o: FilterOption) => {
          if ((option.visible == false) && (datapoint.dataset == option.id)) return true
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

  return null
}

export default Updater
