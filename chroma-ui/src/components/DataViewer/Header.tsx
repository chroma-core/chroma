import React, { useCallback, useEffect } from 'react'
import { Flex, useTheme, Tooltip, useColorModeValue, IconButton, Text, Button } from '@chakra-ui/react'
import { BsCursorFill } from 'react-icons/bs'
import { TbLasso } from 'react-icons/tb'
import ColorToggle from '../ColorToggle'
import ShortcutsDrawer from './ShortcutsDrawer'
import { useAtom } from 'jotai'
import { toolSelectedAtom, toolWhenShiftPressedAtom, cursorAtom, contextObjectSwitcherAtom, DataType, labelDatapointsAtom } from './atoms'
import { CursorMap } from './types'

const Header: React.FC = () => {
  const theme = useTheme()

  const [toolSelected, setToolSelected] = useAtom(toolSelectedAtom)
  const [toolWhenShiftPressed, setToolWhenShiftPressed] = useAtom(toolWhenShiftPressedAtom)
  const [cursor, setCursor] = useAtom(cursorAtom)
  const [contextObjectSwitcher, updatecontextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)
  const [labelDatapoints] = useAtom(labelDatapointsAtom)

  var cursorSelected = toolSelected === 'cursor'
  var lassoSelected = toolSelected === 'lasso'

  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.medium, theme.colors.ch_gray.dark)

  const handleKeyPress = useCallback((event: any) => {
    if (event.key === 'v') moveClicked()
    if (event.key === 'l') lassoClicked()
  }, [])

  useEffect(() => {
    document.addEventListener('keydown', handleKeyPress)
    return () => {
      document.removeEventListener('keydown', handleKeyPress)
    }
  }, [handleKeyPress])

  // Topbar functions passed down
  function moveClicked() {
    setToolSelected('cursor')
    setCursor(CursorMap.select)
  }
  function lassoClicked() {
    setToolSelected('lasso')
    setCursor(CursorMap.lasso)
  }

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
            _hover={cursorSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
            _active={cursorSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
            color={cursorSelected ? 'white' : undefined}
            onClick={moveClicked}
          />
        </Tooltip>
        <Tooltip label='Lasso'>
          <IconButton
            aria-label='lasso'
            icon={<TbLasso />}
            variant='ghost'
            borderRadius={0}
            height="100%"
            pr={4}
            backgroundColor={lassoSelected ? theme.colors.ch_blue : null}
            _hover={lassoSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
            _active={lassoSelected ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
            color={lassoSelected ? 'white' : undefined}
            onClick={lassoClicked}
            pl={4} />
        </Tooltip>
      </Flex>
      <Flex>
        {(Object.values(labelDatapoints).length > 0) ?
          <>
            <Button
              borderRadius={0}
              height="100%"
              variant='ghost'
              color={(contextObjectSwitcher == DataType.Context) ? 'white' : undefined}
              _hover={(contextObjectSwitcher == DataType.Context) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
              _active={(contextObjectSwitcher == DataType.Context) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
              backgroundColor={(contextObjectSwitcher == DataType.Context) ? theme.colors.ch_blue : null}
              onClick={() => updatecontextObjectSwitcher(DataType.Context)}>
              Context
            </Button>
            <Button
              borderRadius={0}
              height="100%"
              variant='ghost'
              color={(contextObjectSwitcher == DataType.Object) ? 'white' : undefined}
              _hover={(contextObjectSwitcher == DataType.Object) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
              _active={(contextObjectSwitcher == DataType.Object) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
              backgroundColor={(contextObjectSwitcher == DataType.Object) ? theme.colors.ch_blue : null}
              onClick={() => updatecontextObjectSwitcher(DataType.Object)}>
              Objects
            </Button>
          </>
          : null}
      </Flex>
      <Flex>
        <ShortcutsDrawer />
        <ColorToggle />
      </Flex>
    </Flex>
  )
}

export default Header
