// @ts-nocheck
import React, { useCallback, useEffect } from 'react'
import { Flex, Button, useTheme, Tooltip, useColorModeValue, IconButton } from '@chakra-ui/react'
import { ArrowForwardIcon } from '@chakra-ui/icons'
import { BsCursorFill, BsBoundingBox } from 'react-icons/bs'
import { BiSelection } from 'react-icons/bi'
import { MdOutlineDraw } from 'react-icons/md'
import { FaRegHandPaper } from 'react-icons/fa'
import ColorToggle from './ColorToggle'

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
    if (event.key === 'v') {
      moveClicked()
    }
    if (event.key === 'l') {
      lassoClicked()
    }
  }, [])

  useEffect(() => {
    // attach the event listener
    document.addEventListener('keydown', handleKeyPress)

    // remove the event listener
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
            icon={<BsBoundingBox />}
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
        <ColorToggle />
      </Flex>
    </Flex>
  )
}

export default Header
