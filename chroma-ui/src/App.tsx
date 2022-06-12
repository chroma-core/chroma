import './App.css';
import { Helmet, HelmetProvider } from 'react-helmet-async'
import ChromaRouter from './themes/Routes';

function App() {
  return (
    <div>
      <HelmetProvider>
        <Helmet defaultTitle="Chroma" />
      </HelmetProvider>
      <ChromaRouter />
    </div>
  )
}

export default App
