import React from 'react'
import { Routes, Route, BrowserRouter } from 'react-router-dom'
import Projects from './components/Pages/ModelSpaces'
import AppContainer from './components/Containers/AppContainer'

const ChromaRouter: React.FC = () => {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/">
          <Route index element={<AppContainer><Projects /></AppContainer>} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default ChromaRouter
