import React from 'react'
import { Routes, Route, BrowserRouter } from 'react-router-dom'
import Embeddings from '../components/Embeddings'

const ChromaRouter: React.FC = () => {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Embeddings />}></Route>
      </Routes>
    </BrowserRouter>
  )
}

export default ChromaRouter
