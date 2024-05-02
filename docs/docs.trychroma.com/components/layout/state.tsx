import React, { createContext, useContext, useEffect, useState } from 'react';

// Create a context with a default empty state
export const GlobalStateContext = createContext({
  globalStateObject: {},
  setGlobalStateObject: (state: {}) => {},
});

export const GlobalStateProvider = ({ children }) => {
  const [globalStateObject, setGlobalStateObject] = useState(() => {

    let initialState = { 'code-lang': 'python' };

    if (typeof window !== 'undefined') {
      // Attempt to read from local storage
      const localState = localStorage.getItem('globalState');
      let localStateObj = {};
      if (localState) {
        localStateObj = JSON.parse(localState);
      }

      // Merge local storage and query params
      initialState = { ...initialState, ...localStateObj};
    }

    return initialState;
  });

  useEffect(() => {
    // Ensure window is defined before attempting to use localStorage
    if (typeof window !== 'undefined') {
      localStorage.setItem('globalState', JSON.stringify(globalStateObject));
    }

  }, [globalStateObject]);

  return (
    <GlobalStateContext.Provider value={{ globalStateObject, setGlobalStateObject }}>
      {children}
    </GlobalStateContext.Provider>
  );
};
