import React from 'react'
import { Routes, Route, BrowserRouter } from 'react-router-dom'
import App from './App'
import Embeddings from './components/Embeddings'
import EmbeddingSet from './components/EmbeddingSets/EmbeddingSet'
import EmbeddingSets from './components/EmbeddingSets/EmbeddingSets'
import Home from './components/Home'
import TestUrql from './components/TestUrql'

const ChromaRouter: React.FC = () => {
  return (
    <BrowserRouter>
      <Routes>
        {/* <Route path="/" element={<Embeddings />}></Route>
        <Route path="/test" element={<TestUrql/>}></Route> */}
        <Route path="/">
          <Route index element={<Home />} />
          <Route path="embedding_sets/:embedding_set_id" element={<EmbeddingSet />} />
          <Route path="embedding_sets" element={<EmbeddingSets />}/>
          <Route path="projection_set/:projection_set_id" element={<Embeddings />} />
        </Route>
      </Routes>
      
    </BrowserRouter>
  )
}

export default ChromaRouter
