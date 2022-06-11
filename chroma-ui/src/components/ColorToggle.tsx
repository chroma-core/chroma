import React from 'react'
import { Button, useColorMode } from '@chakra-ui/react'
import { MdDarkMode, MdLightMode } from 'react-icons/md';

function ColorToggle() {
  const { colorMode, toggleColorMode } = useColorMode()
  return (
    <header>
      <Button onClick={toggleColorMode} variant="ghost" borderRadius={0} height="100%" >
        {colorMode === 'light' ?
          <MdLightMode />
          :
          <MdDarkMode />
        }
      </Button>
    </header>
  )
}

export default ColorToggle
