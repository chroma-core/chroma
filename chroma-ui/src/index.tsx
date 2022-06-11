import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import defaultTheme from './themes/defaultTheme'
import { BrowserRouter } from 'react-router-dom'

import { extendTheme, ChakraProvider } from '@chakra-ui/react'
import { Global, css } from '@emotion/react';

const GlobalStyles = css`
  /*
    This will hide the focus indicator if the element receives focus via the mouse,
    but it will still show up on keyboard focus.
  */
  .js-focus-visible :focus:not([data-focus-visible-added]) {
    outline: none;
    box-shadow: none;
  }
`
const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

// wonderig why we render everything twice? it's intentional, but only happens in dev mode
// https://stackoverflow.com/questions/48846289/why-is-my-react-component-is-rendering-twice
root.render(
  // <React.StrictMode>
    <ChakraProvider theme={defaultTheme}>
      <Global styles={GlobalStyles} />
      <App />
    </ChakraProvider>
  // </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();

