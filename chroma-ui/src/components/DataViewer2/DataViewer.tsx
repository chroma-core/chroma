import React, { useEffect, useState } from 'react';
import { useTheme, Text } from '@chakra-ui/react'
import { useParams } from 'react-router-dom';
import { useQuery } from 'urql';
import ExplorerContainer from '../Containers/ExplorerContainer';
import DataPanel from './DataPanel';
import { getDatapointsForProject, GetProjectAndProjectionSets, getProjectionsForProjectionSet } from './DataViewerApi';
import { getMostRecentCreatedAt } from './DataViewerUtils';
import { useAtom } from 'jotai';
import { datapointsAtom, labelsAtom, tagsAtom, resourcesAtom, inferencesAtom, datasetsAtom, categoriesAtom, projectionsAtom, selectedDatapointsAtom, toolSelectedAtom, toolWhenShiftPressedAtom, cursorAtom, inferenceFilterAtom, categoryFilterAtom } from './atoms';
import { NormalizeData, CursorMap, Filter, FilterType, FilterOption } from './types';
import Header from './Header';
import FilterSidebar from './FilterSidebar';
import Updater from './Updater';
import ProjectionPlotter from './ProjectionPlotter';

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
  const [toolSelected, setToolSelected] = useAtom(toolSelectedAtom)
  const [toolWhenShiftPressed, setToolWhenShiftPressed] = useAtom(toolWhenShiftPressedAtom)
  const [cursor, setCursor] = useAtom(cursorAtom)

  const hydrateAtoms = (normalizedData: NormalizeData) => {
    console.log('normalizedData!!!', normalizedData)
    updatedatapoints(normalizedData.entities.datapoints)
    updatedatasets(normalizedData.entities.datasets)
    updatelabels(normalizedData.entities.labels)
    updatetags(normalizedData.entities.tags)
    updateresources(normalizedData.entities.resources)
    updateinferences(normalizedData.entities.inferences)
    updatecategories(normalizedData.entities.categories)
    updateprojections(normalizedData.entities.projections)
    // build filters....... 
  }

  // once complete, fetch datapoints for the project, and the most recent set of projections
  useEffect(() => {
    getDatapointsForProject(projectId, hydrateAtoms);
  }, []);

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
    </div>
  )
}

export default DataViewer