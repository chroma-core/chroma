import React from 'react'
import ReactDOM from 'react-dom/client'
import './index.css'
import reportWebVitals from './reportWebVitals'
import defaultTheme from './themes/defaultTheme'
import { BrowserRouter } from 'react-router-dom'

import { extendTheme, ChakraProvider, ColorModeScript } from '@chakra-ui/react'
import { Global, css } from '@emotion/react';

import { createClient, Provider, defaultExchanges, subscriptionExchange } from 'urql';
// import { SubscriptionClient } from 'subscriptions-transport-ws';
import ChromaRouter from './Routes'
import { HelmetProvider, Helmet } from 'react-helmet-async'

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

console.log(`%c
  _____     __  __     ______     ______     __    __     ______    \r\n\/\\  ___\\   \/\\ \\_\\ \\   \/\\  == \\   \/\\  __ \\   \/\\ \"-.\/  \\   \/\\  __ \\   \r\n\\ \\ \\____  \\ \\  __ \\  \\ \\  __<   \\ \\ \\\/\\ \\  \\ \\ \\-.\/\\ \\  \\ \\  __ \\  \r\n \\ \\_____\\  \\ \\_\\ \\_\\  \\ \\_\\ \\_\\  \\ \\_____\\  \\ \\_\\ \\ \\_\\  \\ \\_\\ \\_\\ \r\n  \\\/_____\/   \\\/_\/\\\/_\/   \\\/_\/ \/_\/   \\\/_____\/   \\\/_\/  \\\/_\/   \\\/_\/\\\/_\/ 
`, `font-family: monospace`);

// const subscriptionClient = new SubscriptionClient('ws://localhost:8000/graphql', { reconnect: true });

const client = createClient({
  url: 'http://localhost:8000/graphql',
  exchanges: [
    ...defaultExchanges,
    // subscriptionExchange({
    //   forwardSubscription: (operation) => subscriptionClient.request(operation)
    // }),
  ],
});

const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement)
root.render(
  <React.StrictMode>
    <ChakraProvider theme={defaultTheme}>
      <HelmetProvider>
        <Helmet defaultTitle="Chroma" />
      </HelmetProvider>
      <Global styles={GlobalStyles} />
      <ColorModeScript initialColorMode="light" />
      <Provider value={client}>
        <ChromaRouter />
      </Provider>
    </ChakraProvider>
  </React.StrictMode>
)

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals()
