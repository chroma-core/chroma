import * as CSS from 'csstype'
import { Flex, Button, useTheme, Icon, Box, IconButton } from '@chakra-ui/react'
import { BsCircleFill, BsFillSquareFill, BsXLg, BsSquare } from 'react-icons/bs';
import { AiOutlineEye, AiOutlineEyeInvisible } from 'react-icons/ai';
import { useState } from "react";
import { IconType } from "react-icons";
import { TbLasso } from 'react-icons/tb';
import { GiSelect } from 'react-icons/gi'

const IconMap: any = {
  circle: BsCircleFill,
  cross: BsXLg,
  square: BsFillSquareFill,
  square_outline: BsSquare,
  show: AiOutlineEye,
  hide: AiOutlineEyeInvisible,
  select: GiSelect
}

interface SidebarButtonProps {
  symbol: 'square' | 'cross' | 'circle' | 'square_outline'
  text: string
  color: CSS.Property.Color
  indent: number,
  showHide?: ({ }) => void,
  selectBy?: ({ }) => void,
  visible: boolean,
  classTitle: string
  keyName: string
}

const SidebarButton: React.FC<SidebarButtonProps> = ({ keyName, symbol, text, color, indent, showHide, selectBy, visible = true, classTitle }) => {
  var icon: string = (visible === true) ? 'show' : 'hide'
  var iconOpp: string = (visible === true) ? 'hide' : 'show'
  var opacity: string = (visible === true) ? "100%" : "20%"

  const [isHovered, setIsHovered] = useState(false);
  const handleMouseEnter = () => {
    setIsHovered(true)
  }
  const handleMouseLeave = () => {
    setIsHovered(false)
  }

  function showHideFn(event: any) {
    event.stopPropagation()
    if (showHide)
      showHide({
        text: text,
        classTitle: classTitle,
      })
  }
  function selectByFn(event: any) {
    event.stopPropagation()
    if (selectBy)
      selectBy({
        text: text,
        classTitle: classTitle,
      })
  }

  let eyeButtonOpacity = "0%"
  if (isHovered) eyeButtonOpacity = "100%"
  if (!visible && isHovered) eyeButtonOpacity = "0%"

  return (
    <Button
      key={keyName}
      onClick={showHideFn}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      justifyContent="flex-start" variant='ghost' size='sm' ml={indent}>
      <Flex justify="space-between" wrap="wrap" width="100%">
        <Box opacity={opacity}>
          <Icon h={3} as={IconMap[symbol] as any} color={color} mr={2} />
          {text}
        </Box>
        <Flex>
          <Icon
            _hover={{ backgroundColor: "rgba(0,0,0,0)" }}
            _active={{ backgroundColor: "rgba(0,0,0,0)" }}
            onClick={selectByFn}
            height="100%"
            opacity={eyeButtonOpacity}
            variant="ghost" aria-label='ShowHide' as={IconMap.select as any} />
        </Flex>
      </Flex>
    </Button>
  )
}

export default SidebarButton
