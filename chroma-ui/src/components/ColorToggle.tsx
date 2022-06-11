import React from 'react'
import { Button, useColorMode } from '@chakra-ui/react'

function ColorToggle() {
  const { colorMode, toggleColorMode } = useColorMode()
  return (
    <header>
      <Button onClick={toggleColorMode}>Toggle {colorMode === 'light' ? 'Dark' : 'Light'}</Button>
    </header>
  )
}

export default ColorToggle
