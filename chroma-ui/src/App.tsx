import './App.css'
import { Helmet } from 'react-helmet'
import ChromaRouter from './themes/Routes'

function App() {
  return (
    <div>
      <Helmet defaultTitle="Chroma" />
      <ChromaRouter />
    </div>
  )
}

export default App
