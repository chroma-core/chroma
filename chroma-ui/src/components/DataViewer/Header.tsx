// @ts-nocheck
import React, { useCallback, useEffect } from 'react'
import { Flex, useTheme, Tooltip, useColorModeValue, IconButton } from '@chakra-ui/react'
import { BsCursorFill } from 'react-icons/bs'
import { TbLasso } from 'react-icons/tb'
import ColorToggle from '../ColorToggle'
import ShortcutsDrawer from './ShortcutsDrawer'

interface HeaderProps {
  toolSelected: string
  moveClicked: (classtring: string) => void
  lassoClicked: (typestring: string) => void
}

const Header: React.FC<HeaderProps> = ({ moveClicked, lassoClicked, toolSelected }) => {
  const theme = useTheme()
  var cursorSelected = toolSelected === 'cursor'
  var lassoSelected = toolSelected === 'lasso'

  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.medium, theme.colors.ch_gray.dark)

  const handleKeyPress = useCallback((event) => {
    if (event.key === 'v') moveClicked()
    if (event.key === 'l') lassoClicked()
  }, [])

  useEffect(() => {
    document.addEventListener('keydown', handleKeyPress)
    return () => {
      document.removeEventListener('keydown', handleKeyPress)
    }
  }, [handleKeyPress])

  return (
    <Flex
      as="header"
      position="fixed"
      w="100%"
      bg={bgColor}
      height="48px"
      borderBottom="1px"
      borderColor={borderColor}
      zIndex={10}
      p={0}
      justifyContent="space-between"
    >
      <Flex>
        <Tooltip label='Select'>
          <IconButton aria-label='Select' icon={<BsCursorFill style={{ transform: "rotate(-90deg)" }} />}
            borderRadius={0}
            height="100%"
            pr={4}
            pl={4}
            variant='ghost'
            backgroundColor={cursorSelected ? theme.colors.ch_blue : null}
            _hover={cursorSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : null}
            _active={cursorSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : null}
            color={cursorSelected ? 'white' : null}
            onClick={moveClicked}
          />
        </Tooltip>
        <Tooltip label='Lasso'>
          <IconButton
            icon={<TbLasso />}
            variant='ghost'
            borderRadius={0}
            height="100%"
            pr={4}
            backgroundColor={lassoSelected ? theme.colors.ch_blue : null}
            _hover={lassoSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : null}
            _active={lassoSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : null}
            color={lassoSelected ? 'white' : null}
            onClick={lassoClicked}
            pl={4} />
        </Tooltip>
      </Flex>
      <Flex>
        <ShortcutsDrawer />
        <ColorToggle />
      </Flex>
    </Flex>
  )
}

export default Header
