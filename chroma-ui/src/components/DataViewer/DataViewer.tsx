import React, { useEffect, useState } from 'react';
import { useTheme, Text, Button, Modal, ModalBody, ModalContent, ModalHeader, ModalOverlay, Progress, Center } from '@chakra-ui/react'
import { useParams } from 'react-router-dom';
import { useQuery } from 'urql';
import ExplorerContainer from '../Containers/ExplorerContainer';
import DataPanel from './DataPanel';
import { getDatapointsForProject, GetProjectAndProjectionSets, getProjectionsForProjectionSet, getTotalDatapointsToFetch } from './DataViewerApi';
import { getMostRecentCreatedAtObjectContext } from './DataViewerUtils';
import { atom, useAtom } from 'jotai';
import {
  context__datapointsAtom, context__labelsAtom, context__tagsAtom, context__resourcesAtom, context__inferencesAtom, context__datasetsAtom, context__categoriesAtom, context__projectionsAtom, selectedDatapointsAtom, toolSelectedAtom, toolWhenShiftPressedAtom, cursorAtom, context__metadataFiltersAtom, globalDatapointAtom,
  object__labelsAtom, object__tagsAtom, object__resourcesAtom, object__datasetsAtom, object__categoriesAtom, object__projectionsAtom, object__metadataFiltersAtom, object__datapointsAtom, context__inferencecategoriesAtom, object__inferencecategoriesAtom
} from './atoms';
import { NormalizeData, CursorMap, Filter, FilterType, FilterOption, Projection, Category, Datapoint } from './types';
import Header from './Header';
import FilterSidebar from './FilterSidebar';
import Updater from './Updater';
import ProjectionPlotter from './ProjectionPlotter';
import distinctColors from 'distinct-colors';
import chroma from 'chroma-js';

/**
 * Simple object check.
 * @param item
 * @returns {boolean}
 */
// @ts-ignore
export function isObject(item) {
  return (item && typeof item === 'object' && !Array.isArray(item));
}

/**
 * Deep merge two objects.
 * @param target
 * @param ...sources
 */
// @ts-ignore
export function mergeDeep(target, ...sources) {
  if (!sources.length) return target;
  const source = sources.shift();

  if (isObject(target) && isObject(source)) {
    for (const key in source) {
      if (isObject(source[key])) {
        if (!target[key]) Object.assign(target, { [key]: {} });
        mergeDeep(target[key], source[key]);
      } else {
        Object.assign(target, { [key]: source[key] });
      }
    }
  }

  return mergeDeep(target, ...sources);
}

function resetIds(start: number, object: {}) {
  var iterator = start
  var returnObj = {}
  Object.keys(object).forEach(key => {
    // @ts-ignore
    returnObj[iterator] = object[key]
    // @ts-ignore
    returnObj[iterator].id = iterator
    iterator += 1
  })
  return returnObj
}

function resetDatapointIds(start: number, resourceStart: number, object: {}) {
  var iterator = start
  var returnObj = {}
  Object.keys(object).forEach(key => {
    // @ts-ignore
    returnObj[iterator] = object[key]
    // @ts-ignore
    returnObj[iterator].id = iterator
    // @ts-ignore
    returnObj[iterator].resource_id += resourceStart
    iterator += 1
  })
  return returnObj
}

function bumpIds(start: number, object: {}) {
  var returnObj = {}
  Object.keys(object).forEach(key => {
    // @ts-ignore 
    // @ts-ignore 
    returnObj[key] = object[key]
    // @ts-ignore
    if (object[key].datapoint_ids === undefined) return
    // @ts-ignore
    returnObj[key].datapoint_ids = object[key].datapoint_ids.slice().map(dp => dp + start)
    // iterator += 1
    // @ts-ignore
  })
  return returnObj
}

const SERVER_PAGE_SIZE = 10000


const DataViewer = () => {
  const theme = useTheme()
  let params = useParams();
  const projectId = parseInt(params.project_id!, 10)

  // Atoms
  const [context__datapoints, updatedatapoints] = useAtom(globalDatapointAtom)
  const [context__labels, updatelabels] = useAtom(context__labelsAtom)
  const [context__tags, updatetags] = useAtom(context__tagsAtom)
  const [context__resources, updateresources] = useAtom(context__resourcesAtom)
  const [context__inferences, updateinferences] = useAtom(context__inferencesAtom)
  const [context__datasets, updatedatasets] = useAtom(context__datasetsAtom)
  const [context__categories, updatecategories] = useAtom(context__categoriesAtom)
  const [context__projections, updateprojections] = useAtom(context__projectionsAtom)
  const [context__metadataFilters, updateMetadataFilters] = useAtom(context__metadataFiltersAtom)

  const [object__datapoints, updateobjectdatapoints] = useAtom(object__datapointsAtom)
  const [object__labels, updateobjectlabels] = useAtom(object__labelsAtom)
  const [object__tags, updateobjecttags] = useAtom(object__tagsAtom)
  const [object__resources, updateobjectresources] = useAtom(object__resourcesAtom)
  // const [object__inferences, updateobjectinferences] = useAtom(labelInferenceFilterAtom)
  const [object__datasets, updateobjectdatasets] = useAtom(object__datasetsAtom)
  const [object__categories, updateobjectcategories] = useAtom(object__categoriesAtom)
  const [object__projections, updateobjectprojections] = useAtom(object__projectionsAtom)
  const [object__metadataFilters, updateobjectMetadataFilters] = useAtom(object__metadataFiltersAtom)

  const [context__inferencecategories, updatecontextinferencecategories] = useAtom(context__inferencecategoriesAtom)
  const [object__inferencecategories, updateobjectinferencecategories] = useAtom(object__inferencecategoriesAtom)

  // UI State
  const [totalDatapointsToFetch, setTotalDatapointsToFetch] = useState<number | null>(null)
  const [datapointsFetched, setDatapointsFetched] = useState<number>(0)
  const [processingDatapoints, setProcessingDatapoints] = useState<boolean>(false)
  const [processingProjections, setProcessingProjections] = useState<boolean>(false)
  const [toolSelected, setToolSelected] = useAtom(toolSelectedAtom)
  const [toolWhenShiftPressed, setToolWhenShiftPressed] = useAtom(toolWhenShiftPressedAtom)
  const [cursor, setCursor] = useAtom(cursorAtom)
  const allFetched = (datapointsFetched == totalDatapointsToFetch)

  // Onload Fetch projects and projection sets
  const [result, reexecuteQuery] = useQuery({
    query: GetProjectAndProjectionSets,
    variables: { "filter": { "projectId": projectId }, "projectId": projectId }
  })
  const { data, fetching, error } = result;

  // once complete, fetch datapoints for the project, and the most recent set of projections
  useEffect(() => {
    if (result.data === undefined) return
    if (allFetched != true) return
    if (result.data.projectionSets.length === 0) {
      // normally would return, but we are going to shove in some data here...... 
      // let stubbedProjections: any[] = []
      // Object.values(datapoints).map(dp => {
      //   stubbedProjections.push({
      //     id: dp.id,
      //     x: Math.random() * 100,
      //     y: Math.random() * 100,
      //     embedding: {
      //       datapoint_id: dp.id
      //     }
      //   })
      // })
      // projectionsWorker.postMessage({ projections: stubbedProjections, datapoints: datapoints })
      // projectionsWorker.onmessage = (e: MessageEvent) => {
      //   updatedatapoints({ ...{ ...datapoints }, ...e.data.datapoints })
      //   updateprojections(e.data.projections)
      //   setProcessingProjections(false)
      //   setProcessingDatapoints(false)
      // }

      // let labelstubbedProjections: any[] = []
      // Object.values(labeldatapoints).map(dp => {
      //   labelstubbedProjections.push({
      //     id: dp.id,
      //     x: Math.random() * 100,
      //     y: Math.random() * 100,
      //     embedding: {
      //       datapoint_id: dp.id
      //     }
      //   })
      // })
      // projectionsWorker2.postMessage({ projections: labelstubbedProjections, datapoints: labeldatapoints })
      // projectionsWorker2.onmessage = (e: MessageEvent) => {
      //   updatecontextdatapoints({ ...{ ...labeldatapoints }, ...e.data.datapoints })
      //   updatelabelprojections(e.data.projections)
      //   setProcessingProjections(false)
      //   setProcessingDatapoints(false)
      // }
      return
    }

    setProcessingProjections(true)

    const projectionsSetsToFetch = getMostRecentCreatedAtObjectContext(result.data.projectionSets)

    for (let index = 0; index < projectionsSetsToFetch.length; index++) {
      let projectionsWorker: Worker = new Worker('/workers/processProjections.js')
      getProjectionsForProjectionSet(projectionsSetsToFetch[index].id, (projectionsResponse: any) => {
        let contextObjectDatapoints
        if (projectionsResponse.setType == 'object') {
          contextObjectDatapoints = object__datapoints
        } else {
          contextObjectDatapoints = context__datapoints
        }
        projectionsWorker.postMessage({ projections: projectionsResponse, datapoints: contextObjectDatapoints })
        projectionsWorker.onmessage = (e: MessageEvent) => {
          // console.log('onmessage callback', e)
          if (e.data.setType == 'object') {
            console.log('setting object projections')
            updateobjectprojections(e.data.projections)
          } else {
            console.log('setting context projections')
            updateprojections(e.data.projections)
          }
          if (e.data.setType == 'object') {
            updateobjectdatapoints({ ...{ ...object__datapoints }, ...e.data.datapoints })
          } else {
            updatedatapoints({ ...{ ...context__datapoints }, ...e.data.datapoints })
          }

          setProcessingProjections(false)
        }
      });

    }


  }, [datapointsFetched]);

  const hydrateAtoms = (normalizedData: any, len: number, prevPage: number) => {

    // since our object__ ids are created on the fly, but our webworker is currently ignorant of existing ides
    // when paging in new data, we want to adjust the ids so they don't collide with existing data

    // @ts-ignore
    normalizedData.object__datapoints = resetDatapointIds(Object.values(object__datapoints).length, Object.values(object__resources).length, normalizedData.object__datapoints)
    normalizedData.object__resources = resetIds(Object.values(object__resources).length, normalizedData.object__resources)
    normalizedData.object__labels = resetIds(Object.values(object__labels).length, normalizedData.object__labels)
    normalizedData.object__categories = bumpIds(Object.values(object__datapoints).length, normalizedData.object__categories)

    // for new data that is paged in, we want to merge it in cleanly...... 
    // but deep merge doesn't work, so we have to do that manually for a bunch of objects

    // deep merge datapoint id lists for tags and categories and datasets
    Object.keys(normalizedData.context__categories).map((item: any, index: number) => {
      let category = context__categories[item]
      let existing = (category !== undefined) ? category.datapoint_ids : []
      let newVals = (normalizedData.context__categories[item].datapoint_ids !== undefined) ? normalizedData.context__categories[item].datapoint_ids : []
      normalizedData.context__categories[item].datapoint_ids = [...newVals, ...existing]
    })
    Object.keys(normalizedData.context__inferenceCategories).map((item: any, index: number) => {
      let category = context__inferencecategories[item]
      let existing = (category !== undefined) ? category.datapoint_ids : []
      let newVals = (normalizedData.context__inferenceCategories[item].datapoint_ids !== undefined) ? normalizedData.context__inferenceCategories[item].datapoint_ids : []
      normalizedData.context__inferenceCategories[item].datapoint_ids = [...newVals, ...existing]
    })
    Object.keys(normalizedData.object__categories).map((item: any, index: number) => {
      let category = object__categories[item]
      let existing = (category !== undefined) ? category.datapoint_ids : []
      let newVals = (normalizedData.object__categories[item].datapoint_ids !== undefined) ? normalizedData.object__categories[item].datapoint_ids : []
      normalizedData.object__categories[item].datapoint_ids = [...newVals, ...existing]
    })
    Object.keys(normalizedData.context__tags).map((key: any, index: number) => {
      let item = context__tags[key]
      let existing = (item !== undefined) ? item.datapoint_ids : []
      normalizedData.context__tags[key].datapoint_ids = [...normalizedData.context__tags[key].datapoint_ids, ...existing]
    })
    Object.keys(normalizedData.object__tags).map((key: any, index: number) => {
      let item = object__tags[key]
      let existing = (item !== undefined) ? item.datapoint_ids : []
      normalizedData.object__tags[key].datapoint_ids = [...normalizedData.object__tags[key].datapoint_ids, ...existing]
    })
    Object.keys(normalizedData.context__datasets).map((key: any, index: number) => {
      let item = context__datasets[key]
      let existing = (item !== undefined) ? item.datapoint_ids : []
      normalizedData.context__datasets[key].datapoint_ids = [...normalizedData.context__datasets[key].datapoint_ids, ...existing]
    })
    // TODO: do this for object__datasets?

    // Now we want to take our metadata filters and post process them

    Object.keys(normalizedData.context__metadataFilters).map((key: any, index: number) => {
      let item = context__metadataFilters[key]
      if (item === undefined) item = { options: {} }

      // see if all the options are numbers, and if so change this over to a range slider from a discrete pick list
      var allNumbers = Object.values(normalizedData.context__metadataFilters[key].options).map((op: any) => !(typeof op.id === 'number')).includes(false)
      if (allNumbers) {
        normalizedData.context__metadataFilters[key].type = FilterType.Continuous
        normalizedData.context__metadataFilters[key].range = { min: Infinity, max: -Infinity, minVisible: Infinity, maxVisible: -Infinity }
        Object.values(normalizedData.context__metadataFilters[key].options).map((op: any) => {
          if (op.id < normalizedData.context__metadataFilters[key].range.min) {
            normalizedData.context__metadataFilters[key].range.min = op.id
            normalizedData.context__metadataFilters[key].range.minVisible = normalizedData.context__metadataFilters[key].range.min
          }
          if (op.id > normalizedData.context__metadataFilters[key].range.max) {
            normalizedData.context__metadataFilters[key].range.max = op.id
            normalizedData.context__metadataFilters[key].range.maxVisible = normalizedData.context__metadataFilters[key].range.max
          }
        })
      }
      normalizedData.context__metadataFilters[key].fetchFn = (datapoint: any) => {
        return datapoint.metadata[normalizedData.context__metadataFilters[key].name]
      }

      // deep merge datapoints ids with existing data
      Object.values(item.options).map((option: any) => {
        let item2 = item.linkedAtom[option.id]
        let existing = (item2 !== undefined) ? item2.datapoint_ids : []
        normalizedData.context__metadataFilters[key].linkedAtom[option.id].datapoint_ids = [...normalizedData.context__metadataFilters[key].linkedAtom[option.id].datapoint_ids, ...existing]
      })

      // convert from object key-value to array
      normalizedData.context__metadataFilters[key].options = Object.values(normalizedData.context__metadataFilters[key].options)

      // add the eval function for this metadata filter
      if (normalizedData.context__metadataFilters[key].type == FilterType.Discrete) {
        var colors = distinctColors({
          "count": normalizedData.context__metadataFilters[key].options.length,
          "lightMin": 20,
          "lightMax": 85,
          "chromaMin": 50
        }).map(color => color.hex())
        normalizedData.context__metadataFilters[key].options.map((option: any, i: number) => {
          option.color = colors[i]
          option.evalDatapoint = (datapoint: Datapoint, o: FilterOption) => {
            // @ts-ignore
            if ((option.visible == false) && (datapoint.metadata[key] == option.id)) return true
            else return false
          }
        })
      } else if (normalizedData.context__metadataFilters[key].type == FilterType.Continuous) {
        normalizedData.context__metadataFilters[key].range.colorScale = chroma.scale(["5B68A8", "5CC8C6", "87DF9C", "E4ED58", "F8EB49", "FACE31", "F79A17", "DE500F"]).colors(50)
        normalizedData.context__metadataFilters[key].options.map((option: any) => {
          option.evalDatapoint = (datapoint: Datapoint, o: FilterOption, f: Filter) => {
            // @ts-ignore
            if ((datapoint.metadata[key] >= f.range.maxVisible) || (datapoint.metadata[key] <= f.range.minVisible)) {
              return true
            }
            else return false
          }
        })
      }
    })

    Object.keys(normalizedData.object__metadataFilters).map((key: any, index: number) => {
      let item = context__metadataFilters[key]
      if (item === undefined) item = { options: {} }

      // see if all the options are numbers, and if so change this over to a range slider from a discrete pick list
      var allNumbers = Object.values(normalizedData.object__metadataFilters[key].options).map((op: any) => !(typeof op.id === 'number')).includes(false)
      if (allNumbers) {
        normalizedData.object__metadataFilters[key].type = FilterType.Continuous
        normalizedData.object__metadataFilters[key].range = { min: Infinity, max: -Infinity, minVisible: Infinity, maxVisible: -Infinity }
        Object.values(normalizedData.object__metadataFilters[key].options).map((op: any) => {
          if (op.id < normalizedData.object__metadataFilters[key].range.min) {
            normalizedData.object__metadataFilters[key].range.min = op.id
            normalizedData.object__metadataFilters[key].range.minVisible = normalizedData.object__metadataFilters[key].range.min
          }
          if (op.id > normalizedData.object__metadataFilters[key].range.max) {
            normalizedData.object__metadataFilters[key].range.max = op.id
            normalizedData.object__metadataFilters[key].range.maxVisible = normalizedData.object__metadataFilters[key].range.max
          }
        })
      }
      normalizedData.object__metadataFilters[key].fetchFn = (datapoint: any) => {
        return datapoint.annotations[0].metadata[normalizedData.object__metadataFilters[key].name]
      }

      // deep merge datapoints ids with existing data
      Object.values(item.options).map((option: any) => {
        let item2 = item.linkedAtom[option.id]
        let existing = (item2 !== undefined) ? item2.datapoint_ids : []
        normalizedData.object__metadataFilters[key].linkedAtom[option.id].datapoint_ids = [...normalizedData.object__metadataFilters[key].linkedAtom[option.id].datapoint_ids, ...existing]
      })

      // convert from object key-value to array
      normalizedData.object__metadataFilters[key].options = Object.values(normalizedData.object__metadataFilters[key].options)

      // add the eval function for this metadata filter
      if (normalizedData.object__metadataFilters[key].type == FilterType.Discrete) {
        var colors = distinctColors({
          "count": normalizedData.object__metadataFilters[key].options.length,
          "lightMin": 20,
          "lightMax": 85,
          "chromaMin": 50
        }).map(color => color.hex())

        normalizedData.object__metadataFilters[key].options.map((option: any, i: number) => {
          option.color = colors[i]
          option.evalDatapoint = (datapoint: Datapoint, o: FilterOption) => {
            // @ts-ignore
            if ((option.visible == false) && (datapoint.annotations[0].metadata[key] == option.id)) return true
            else return false
          }
        })

      } else if (normalizedData.object__metadataFilters[key].type == FilterType.Continuous) {
        normalizedData.object__metadataFilters[key].range.colorScale = chroma.scale(["5B68A8", "5CC8C6", "87DF9C", "E4ED58", "F8EB49", "FACE31", "F79A17", "DE500F"]).colors(50)

        normalizedData.object__metadataFilters[key].options.map((option: any) => {
          option.evalDatapoint = (datapoint: Datapoint, o: FilterOption, f: Filter) => {
            // @ts-ignore
            if ((datapoint.annotations[0].metadata[key] >= f.range.maxVisible) || (datapoint.annotations[0].metadata[key] <= f.range.minVisible)) {
              return true
            }
            else return false
          }
        })
      }
    })

    updateMetadataFilters({ ...{ ...context__metadataFilters }, ...normalizedData.context__metadataFilters })
    updatedatapoints({ ...{ ...context__datapoints }, ...normalizedData.context__datapoints })
    updatedatasets({ ...{ ...context__datasets }, ...normalizedData.context__datasets })
    updatelabels({ ...{ ...context__labels }, ...normalizedData.context__labels })
    updateresources({ ...{ ...context__resources }, ...normalizedData.context__resources })
    updateinferences({ ...{ ...context__inferences }, ...normalizedData.context__inferences })
    updatetags({ ...{ ...context__tags }, ...normalizedData.context__tags })
    updatecategories({ ...{ ...context__categories }, ...normalizedData.context__categories })

    updatecontextinferencecategories({ ...{ ...context__inferencecategories }, ...normalizedData.context__inferenceCategories })

    updateobjectMetadataFilters({ ...{ ...object__metadataFilters }, ...normalizedData.object__metadataFilters })
    updateobjectdatapoints({ ...{ ...object__datapoints }, ...normalizedData.object__datapoints })
    updateobjectdatasets({ ...{ ...object__datasets }, ...normalizedData.object__datasets })
    updateobjectlabels({ ...{ ...object__labels }, ...normalizedData.object__labels })
    updateobjectresources({ ...{ ...object__resources }, ...normalizedData.object__resources })
    updateobjecttags({ ...{ ...object__tags }, ...normalizedData.object__tags })
    updateobjectcategories({ ...{ ...object__categories }, ...normalizedData.object__categories })

    setProcessingDatapoints(false)
    setDatapointsFetched(datapointsFetched + len)
  }

  useEffect(() => {
    getTotalDatapointsToFetch(projectId, setTotalDatapointsToFetch);
  }, []);

  // once complete, fetch datapoints for the project, and the most recent set of projections
  useEffect(() => {
    if (totalDatapointsToFetch == null) return
    if (totalDatapointsToFetch == datapointsFetched) return
    const page = Math.ceil(datapointsFetched / SERVER_PAGE_SIZE)
    setProcessingDatapoints(true)
    getDatapointsForProject(projectId, page, hydrateAtoms);
  }, [datapointsFetched, totalDatapointsToFetch]);

  function handleKeyDown(event: any) {
    if ([91, 93, 16].includes(event.keyCode)) { // 16: SHIFT, 91/93: COMMAND left and right
      setToolSelected('lasso')
      setToolWhenShiftPressed(toolSelected)
      if (event.keyCode == 16) setCursor(CursorMap.add)
      if ([91, 93].includes(event.keyCode)) setCursor(CursorMap.remove)
    }
  }

  function handleKeyUp(event: any) {
    if ([91, 93, 16].includes(event.keyCode)) { // 16: SHIFT, 91/93: COMMAND left and right
      if (toolWhenShiftPressed !== 'lasso') {
        setToolSelected(toolWhenShiftPressed)
        setToolWhenShiftPressed('')
        setCursor(CursorMap.select)
      }
      if (toolWhenShiftPressed === 'lasso') {
        setCursor(CursorMap.lasso)
      }
    }
  }

  let loadingModalString = ""
  const progressModalOpen = !(datapointsFetched == totalDatapointsToFetch) || processingDatapoints || processingProjections
  let progressWidth = 0
  if (totalDatapointsToFetch) progressWidth = ((datapointsFetched) / (totalDatapointsToFetch)) * 100
  if (allFetched) loadingModalString = "Finishing loading...."
  else if (totalDatapointsToFetch === undefined) loadingModalString = "Starting up...."
  else loadingModalString = "Fetched " + datapointsFetched.toLocaleString("en-US") + " / " + totalDatapointsToFetch?.toLocaleString("en-US")

  return (
    // tabIndex is required to fire event https://stackoverflow.com/questions/43503964/onkeydown-event-not-working-on-divs-in-react
    <div
      tabIndex={0}
      onKeyDown={(e) => handleKeyDown(e)}
      onKeyUp={(e) => handleKeyUp(e)}
    >
      <Updater />
      <ExplorerContainer>
        <Header />
        <FilterSidebar showSkeleton={false} />
        <ProjectionPlotter allFetched={allFetched} />
        <DataPanel />
      </ExplorerContainer>

      <Modal isCentered isOpen={progressModalOpen} closeOnOverlayClick={false} onClose={() => { }} autoFocus={true} closeOnEsc={false}>
        <ModalOverlay
          bg='blackAlpha.300'
          backdropFilter='blur(2px)'
        />
        <ModalContent>
          <ModalHeader>Downloading data</ModalHeader>
          <ModalBody pb={10}>
            <Progress value={progressWidth} borderRadius={5} sx={{
              "& > div:first-child": {
                transitionProperty: "width",
                transitionDuration: '4s',
                transitionTimingFunction: 'linear'
              },
            }} />
            <Center pt={3}>
              <Text>{loadingModalString}</Text>
            </Center>
          </ModalBody>
        </ModalContent>
      </Modal>
    </div >
  )
}

export default DataViewer