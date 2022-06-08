// @ts-nocheck

import React, { useEffect, useState } from 'react';
import { useTheme } from '@chakra-ui/react'
import PageContainer from './containers/PageContainer';
import Header from './Header';
import RightSidebar from './RightSidebar';
import LeftSidebar from './LeftSidebar';
import EmbeddingsContainer from './EmbeddingsViewer/EmbeddingsContainer';
import distinctColors from 'distinct-colors'
import chroma from "chroma-js" // nothing to do with us! 
// import testDataArray from './testData';

function getEmbeddings(cb) {
  fetch(`/graphql`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `query fetchAllEmbeddings {
        embeddings {
          data
        }
      }`,
    }),
  })
    .then(res => res.json())
    .then(res => cb(res.data.embeddings.data))
    .catch(console.error)
}

// first we want to find the unique values in our metadata
// and create sets of them
var generateMetadataSets = function(testData) {
  var metadataSets = {}
  testData.forEach(data => {
    // metadata stored in the third place in the array
    for (const [k, v] of Object.entries(data[2])) {
      if (metadataSets[k] === undefined) {
        metadataSets[k] = new Set()
      } 
      metadataSets[k].add(v)
    }
  })
  return metadataSets
}

let colorsOpts = distinctColors({"count": 20})
let colorsUsed = []

// then we want to build a multi-layered object that we will
// use to render the left sidebar
// currently this is opinionated as classes -> types
var generateLeftSidebarObject = function(metadataSets) {
  // right now the ordering of these is very sensitive to the
  // order of the colors passed to scatterplot in scatterplot.tsx
  var classTypeDict = []
  var classOptions = metadataSets['class']
  var typeOptions = metadataSets['type']
  classOptions.forEach(option => {
    var color = colorsOpts.shift()?.hex()
    console.log('color', color)
    colorsUsed.push(color)

    classTypeDict.push({
      'class': option,
      title: option, 
      'subtypes': [],
      visible: true,
      color: color
    })
  })
  classTypeDict.forEach(cClass => {
    typeOptions.forEach(option => {
      cClass.subtypes.push({
        'type': option,
        title: option, 
        visible: true,
        color: cClass.color
      })
    })
  })
  return classTypeDict
}

// then we take the data format that were given by the server for points
// and get it into the format that we can pass to regl-scatterplot
var dataToPlotter = function(testData, classTypeDict) {
  var dataToPlot = []
  testData.forEach(data => {
    // x is in pos 0, and y in pos 1
    // pos3 is opacity (0-1), pos4 is class (int)
    // color map for the classes are set in scatterplot
    var objectIndex = classTypeDict.findIndex((t, index)=>t.title === data[2]['class']);
    dataToPlot.push([data[0], data[1], 1, objectIndex])
  })
  return dataToPlot
}

function Embeddings() {
  const theme = useTheme();

  console.log('colorsUsed', colorsUsed)

  let [points, setPoints] = useState<any>(null);
  let [toolSelected, setToolSelected] = useState<any>('cursor');
  let [cursor, setCursor] = useState('grab');
  let [selectedPoints, setSelectedPoints] = useState([]) // callback from regl-scatterplot
  let [unselectedPoints, setUnselectedPoints] = useState([]) // passed down to regl-scatterplot
  let [classDict, setClassDict] = useState(undefined) // object that renders the left sidebar

  // set up data onload
  useEffect(() => {
    getEmbeddings(data => {
      var serverData = JSON.parse(data)
      var metadataSets = generateMetadataSets(serverData)
      var classTypeDict = generateLeftSidebarObject(metadataSets)
      var dataToPlot = dataToPlotter(serverData, classTypeDict)
      setClassDict(classTypeDict)
      setPoints(dataToPlot)
    } )
  }, []);

  // Callback functions that are fired by regl-scatterplot
  const selectHandler = ({ points: selectedPoints }) => {
    setUnselectedPoints([])
    setSelectedPoints(selectedPoints)
  };
  const deselectHandler = () => {
    console.log('deselected points')
  };

  // Topbar functions passed down
  function moveClicked() {
    setToolSelected('cursor')
    setCursor('grab')
  }
  function lassoClicked() {
    setToolSelected('lasso')
    setCursor('crosshair')
  }

  // Left sidebar functions passed down
  // - these trigger the classes to be hidden or shown
  function classClicked(returnObject: string): void { 
    var objectIndex = classDict.findIndex((t, index)=>t.title == returnObject.text);
    var currentVisibility = classDict[objectIndex].visible
    classDict[objectIndex].visible = !currentVisibility
    classDict[objectIndex].subtypes.forEach((subtype) => subtype.visible = !currentVisibility)
    setClassDict([...classDict])
  }
  function typeClicked(returnObject: string): void { 
    var objectIndex = classDict.findIndex((t, index)=>t.title === returnObject.classTitle);
    var subTypeIndex = classDict[objectIndex].subtypes.findIndex((subtype) => subtype.title === returnObject.text)
    var currentVisibility = classDict[objectIndex].subtypes[subTypeIndex].visible
    classDict[objectIndex].subtypes[subTypeIndex].visible = !currentVisibility
    setClassDict([...classDict])
  }

  // Right sidebar functions passed down
  function clearSelected(points) {
    if (points !== undefined) {
      setUnselectedPoints(points)
    } else {
      setUnselectedPoints(selectedPoints)
    }
  }
  function tagSelected() {
    console.log('tagSelected')
  }

  return (
    <div>
      <PageContainer>
        <Header
          toolSelected={toolSelected}
          moveClicked={moveClicked}
          lassoClicked={lassoClicked}>
        </Header>
        <LeftSidebar 
          classDict={classDict} 
          classClicked={classClicked} 
          typeClicked={typeClicked}>
        </LeftSidebar>
        <EmbeddingsContainer 
          points={points} 
          toolSelected={toolSelected}
          selectHandler={selectHandler}
          deselectHandler={deselectHandler}
          unselectedPoints={unselectedPoints}
          cursor={cursor}
          colors={colorsUsed}
          ></EmbeddingsContainer>
        <RightSidebar 
          selectedPoints={selectedPoints}
          clearSelected={clearSelected}
          tagSelected={tagSelected}
        ></RightSidebar>
      </PageContainer>
    </div>
  );
}

export default Embeddings;
