// @ts-nocheck

import React, { useEffect, useState } from 'react';
import { useTheme } from '@chakra-ui/react'
import PageContainer from './containers/PageContainer';
import Header from './Header';
import RightSidebar from './RightSidebar';
import LeftSidebar from './LeftSidebar';
import EmbeddingsContainer from './EmbeddingsViewer/EmbeddingsContainer';
import distinctColors from 'distinct-colors'
import chroma from "chroma-js" // nothing to do with us! a color library

function getEmbeddings(cb) {
  fetch(`/graphql`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `query fetchAllEmbeddings {
        datapoints {
          datapoints {
            x,
            y,
            metadata
          }
        }
      }`,
    }),
  })
    .then(res => res.json())
    .then(res => {
      cb(res.data.datapoints.datapoints)
    })
    .catch(console.error)
}

// first we want to find the unique values in our metadata
// and create sets of them
var generateMetadataSets = function(testData) {
  var metadataSets = {}
  testData.forEach(data => {
    let metadata = JSON.parse(data.metadata)
    // metadata stored in the third place in the array
    for (const [k, v] of Object.entries(metadata)) {
      if (metadataSets[k] === undefined) {
        metadataSets[k] = new Set()
      } 
      metadataSets[k].add(v)
    }
  })
  return metadataSets
}

// then we want to build a multi-layered object that we will
// use to render the left sidebar
// currently this is opinionated as classes -> types
var generateLeftSidebarObject = function(metadataSets) {
  var numberOfColors = metadataSets['class'].size

  // https://medialab.github.io/iwanthue/
  let colorsOpts = distinctColors({
    "count": numberOfColors, 
    "lightMin": 20,
    "lightMax": 80,
    "chromaMin": 80
  })

  var colors = []
  // right now the ordering of these is very sensitive to the
  // order of the colors passed to scatterplot in scatterplot.tsx
  var classTypeDict = []
  var classOptions = metadataSets['class']
  var typeOptions = metadataSets['type']
  var i = 0;
  classOptions.forEach(option => {
    classTypeDict.push({
      'class': option,
      title: option, 
      'subtypes': [],
      visible: true,
      color: chroma(colorsOpts[i]).hex()
    })

    i++
  })
  classTypeDict.forEach(cClass => {
    typeOptions.forEach(option => {
      let color;
      if (option === 'production') {
        color = chroma(cClass.color).brighten().hex()
      } else if (option === 'test') {
        color = chroma(cClass.color).darken().hex()
      } else {
        color = cClass.color
      }
      colors.push(color)

      cClass.subtypes.push({
        'type': option,
        title: option, 
        visible: true,
        color: color
      })
    })
  })
  return [classTypeDict, colors]
}

// then we take the data format that were given by the server for points
// and get it into the format that we can pass to regl-scatterplot
var dataToPlotter = function(testData, classTypeDict) {
  var dataToPlot = []
  testData.forEach(data => {
    // x is in pos 0, and y in pos 1
    // pos3 is opacity (0-1), pos4 is class (int)
    // color map for the classes are set in scatterplot
    let metadata = JSON.parse(data.metadata)
    var objectIndex = classTypeDict.findIndex((t, index)=>t.title === metadata['class']);
    var typeIndexOffset = classTypeDict[objectIndex].subtypes.findIndex((t, index)=>t.title === data[2]['type'])
    var classVisible = classTypeDict[objectIndex].visible
    var typeVisble = classTypeDict[objectIndex].subtypes[typeIndexOffset].visible

    var opacity = 1
    if (!typeVisble) {
      opacity = 0
    } else if (!classVisible) {
      opacity = 0
    }
    
    dataToPlot.push([data.x, data.y, opacity, (objectIndex*3) + typeIndexOffset])
  })
  return dataToPlot
}

function Embeddings() {
  const theme = useTheme();

  let [serverData, setServerData] = useState<any>([]);
  let [points, setPoints] = useState<any>(null);
  let [toolSelected, setToolSelected] = useState<any>('cursor');
  let [cursor, setCursor] = useState('grab');
  let [selectedPoints, setSelectedPoints] = useState([]) // callback from regl-scatterplot
  let [unselectedPoints, setUnselectedPoints] = useState([]) // passed down to regl-scatterplot
  let [classDict, setClassDict] = useState(undefined) // object that renders the left sidebar
  let [colorsUsed, setColorsUsed] = useState([])

  // set up data onload
  useEffect(() => {
    getEmbeddings(dataFromServer => {
      var metadataSets = generateMetadataSets(dataFromServer)
      var response = generateLeftSidebarObject(metadataSets)
      var classTypeDict = response[0]
      var colors = response[1]
      setColorsUsed(colors)
      
      var dataToPlot = dataToPlotter(dataFromServer, classTypeDict)

      setClassDict(classTypeDict)
      setPoints(dataToPlot)
      setServerData(dataFromServer)
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
    updatePointVisiblity()
  }
  function typeClicked(returnObject: string): void { 
    var objectIndex = classDict.findIndex((t, index)=>t.title === returnObject.classTitle);
    var subTypeIndex = classDict[objectIndex].subtypes.findIndex((subtype) => subtype.title === returnObject.text)
    var currentVisibility = classDict[objectIndex].subtypes[subTypeIndex].visible
    classDict[objectIndex].subtypes[subTypeIndex].visible = !currentVisibility
    setClassDict([...classDict])
    updatePointVisiblity()
  }

  function updatePointVisiblity() {
    setPoints(dataToPlotter(serverData, classDict))
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
          serverData={serverData}
        ></RightSidebar>
      </PageContainer>
    </div>
  );
}

export default Embeddings;
