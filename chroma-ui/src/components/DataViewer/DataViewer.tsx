import React, { useEffect, useState } from 'react';
import { useTheme, Text, Button, Modal, ModalBody, ModalContent, ModalHeader, ModalOverlay, Progress, Center } from '@chakra-ui/react'
import { useParams } from 'react-router-dom';
import { useQuery } from 'urql';
import ExplorerContainer from '../Containers/ExplorerContainer';
import DataPanel from './DataPanel';
import { getDatapointsForProject, GetProjectAndProjectionSets, getProjectionsForProjectionSet, getTotalDatapointsToFetch } from './DataViewerApi';
import { getMostRecentCreatedAt } from './DataViewerUtils';
import { atom, useAtom } from 'jotai';
import { datapointsAtom, labelsAtom, tagsAtom, resourcesAtom, inferencesAtom, datasetsAtom, categoriesAtom, projectionsAtom, selectedDatapointsAtom, toolSelectedAtom, toolWhenShiftPressedAtom, cursorAtom, inferenceFilterAtom, categoryFilterAtom } from './atoms';
import { NormalizeData, CursorMap, Filter, FilterType, FilterOption } from './types';
import Header from './Header';
import FilterSidebar from './FilterSidebar';
import Updater from './Updater';
import ProjectionPlotter from './ProjectionPlotter';

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

const SERVER_PAGE_SIZE = 10000

const DataViewer = () => {
  const theme = useTheme()
  let params = useParams();
  const projectId = parseInt(params.project_id!, 10)

  // Atoms
  const [datapoints, updatedatapoints] = useAtom(datapointsAtom)
  const [labels, updatelabels] = useAtom(labelsAtom)
  const [tags, updatetags] = useAtom(tagsAtom)
  const [resources, updateresources] = useAtom(resourcesAtom)
  const [inferences, updateinferences] = useAtom(inferencesAtom)
  const [datasets, updatedatasets] = useAtom(datasetsAtom)
  const [categories, updatecategories] = useAtom(categoriesAtom)
  const [projections, updateprojections] = useAtom(projectionsAtom)
  const [selectedDatapoints] = useAtom(selectedDatapointsAtom)

  // UI State
  const [totalDatapointsToFetch, setTotalDatapointsToFetch] = useState<number | null>(null)
  const [datapointsFetched, setDatapointsFetched] = useState<number>(0)
  const [processingDatapoints, setProcessingDatapoints] = useState<boolean>(false)
  const [toolSelected, setToolSelected] = useAtom(toolSelectedAtom)
  const [toolWhenShiftPressed, setToolWhenShiftPressed] = useAtom(toolWhenShiftPressedAtom)
  const [cursor, setCursor] = useAtom(cursorAtom)

  const hydrateAtoms = (normalizedData: any, len: number, prevPage: number) => {
    // console.log('hydrateAtoms: normalizedData', normalizedData)
    updatedatapoints({ ...{ ...datapoints }, ...normalizedData.datapoints })
    updatedatasets({ ...{ ...datasets }, ...normalizedData.datasets })
    updatelabels({ ...{ ...labels }, ...normalizedData.labels })
    updateresources({ ...{ ...resources }, ...normalizedData.resources })
    updateinferences({ ...{ ...inferences }, ...normalizedData.inferences })
    updateprojections({ ...{ ...projections }, ...normalizedData.projections })
    updatetags({ ...{ ...tags }, ...normalizedData.tags || {} })
    updatecategories({ ...{ ...categories }, ...normalizedData.categories })
    setDatapointsFetched(datapointsFetched + len)
    setProcessingDatapoints(false)
  }

  useEffect(() => {
    getTotalDatapointsToFetch(projectId, setTotalDatapointsToFetch);
  }, []);

  // once complete, fetch datapoints for the project, and the most recent set of projections
  useEffect(() => {
    if (totalDatapointsToFetch == null) return
    const page = Math.ceil(datapointsFetched / SERVER_PAGE_SIZE)
    setProcessingDatapoints(true)
    console.log('page', page)
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

  const allFetched = (datapointsFetched == totalDatapointsToFetch)
  let loadingModalString = ""
  const progressModalOpen = !(datapointsFetched == totalDatapointsToFetch) || processingDatapoints
  let progressWidth = 0
  if (totalDatapointsToFetch) progressWidth = ((datapointsFetched) / (totalDatapointsToFetch)) * 100
  if (allFetched) loadingModalString = "Finishing loading...."
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
        <ProjectionPlotter />
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