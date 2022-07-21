import React from 'react'
import { Routes, Route, BrowserRouter } from 'react-router-dom'
import Projects from './components/Projects/Projects'
import Project from './components/Projects/Project'
import Datasets from './components/Datasets/Datasets'
import Dataset from './components/Datasets/Dataset'
import Models from './components/Models/Models'
import Model from './components/Models/Model'
import AppContainer from './components/Containers/AppContainer'
import Jobs from './components/Jobs/Jobs'
import Job from './components/Jobs/Job'
import Embeddings from './components/Explorer/Embeddings'
import DataViewer from './components/DataViewer/DataViewer'
import DataFetchTest from './components/DataViewer/DataFetchTest'

const ChromaRouter: React.FC = () => {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/">
          <Route index element={<AppContainer><Projects /></AppContainer>} />
          {/* <Route path="jobs" element={<AppContainer><Jobs /></AppContainer>} />
          <Route path="jobs/:job_id" element={<AppContainer><Job /></AppContainer>} /> */}
          <Route path="test" element={<DataFetchTest />} />
          <Route path="data_viewer/:project_id" element={<DataViewer />} />
          <Route path="projects/:project_id" element={<AppContainer><Project /></AppContainer>} />
          <Route path="projects/:project_id/datasets" element={<AppContainer><Datasets /></AppContainer>} />
          <Route path="projects/:project_id/datasets/:dataset_id" element={<AppContainer><Dataset /></AppContainer>} />
          <Route path="projects/:project_id/models" element={<AppContainer><Models /></AppContainer>} />
          <Route path="projects/:project_id/models/:model_id" element={<AppContainer><Model /></AppContainer>} />
          <Route path="projection_set/:projection_set_id" element={<Embeddings />} />
        </Route>
      </Routes>

    </BrowserRouter>
  )
}

export default ChromaRouter
