import React from 'react'
import { Routes, Route, BrowserRouter } from 'react-router-dom'
import App from './App'
// import Embeddings from './components/Embeddings'
// import EmbeddingSet from './components/EmbeddingSets/EmbeddingSet'
// import EmbeddingSets from './components/EmbeddingSets/EmbeddingSets'
import Projects from './components/Projects/Projects'
import Project from './components/Projects/Project'
import Datasets from './components/Datasets/Datasets'
import Dataset from './components/Datasets/Dataset'
import Models from './components/Models/Models'
import Model from './components/Models/Model'
// import TestUrql from './components/TestUrql'
import AppContainer from './components/Containers/AppContainer'
import Jobs from './components/Jobs/Jobs'
import Job from './components/Jobs/Job'

const ChromaRouter: React.FC = () => {
  return (
    <BrowserRouter>
      <Routes>
        {/* <Route path="/" element={<Embeddings />}></Route>
        <Route path="/test" element={<TestUrql/>}></Route> */}
        <Route path="/">
          <Route index element={<AppContainer><Projects/></AppContainer>} />
          <Route path="jobs" element={<AppContainer><Jobs/></AppContainer>}/>
          <Route path="jobs/:job_id" element={<AppContainer><Job/></AppContainer>}/>
          <Route path="projects/:project_id" element={<AppContainer><Project/></AppContainer>}/>
          <Route path="projects/:project_id/datasets" element={<AppContainer><Datasets /></AppContainer>} />
          <Route path="projects/:project_id/datasets/:dataset_id" element={<AppContainer><Dataset /></AppContainer>} />
          <Route path="projects/:project_id/models" element={<AppContainer><Models /></AppContainer>} />
          <Route path="projects/:project_id/models/:model_id" element={<AppContainer><Model /></AppContainer>} />
          {/* <Route path="embedding_sets/:embedding_set_id" element={<EmbeddingSet />} />
          <Route path="embedding_sets" element={<EmbeddingSets />}/>
          <Route path="projection_set/:projection_set_id" element={<Embeddings />} /> */}
        </Route>
      </Routes>
      
    </BrowserRouter>
  )
}

export default ChromaRouter
