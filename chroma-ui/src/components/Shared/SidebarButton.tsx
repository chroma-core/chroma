import * as CSS from 'csstype'
import { Flex, Button, useTheme, Icon, Box, IconButton, filter, Tag } from '@chakra-ui/react'
import { BsCircleFill, BsFillSquareFill, BsXLg, BsSquare } from 'react-icons/bs';
import { AiOutlineEye, AiOutlineEyeInvisible } from 'react-icons/ai';
import { useState } from "react";
import { IconType } from "react-icons";
import { TbLasso } from 'react-icons/tb';
import { GiSelect } from 'react-icons/gi'
import { MdOutlineKeyboardArrowDown, MdOutlineKeyboardArrowRight } from 'react-icons/md'

const IconMap: any = {
  circle: BsCircleFill,
  cross: BsXLg,
  square: BsFillSquareFill,
  square_outline: BsSquare,
  show: AiOutlineEye,
  hide: AiOutlineEyeInvisible,
  select: GiSelect,
  open: MdOutlineKeyboardArrowDown,
  closed: MdOutlineKeyboardArrowRight
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
  isExpanded?: boolean
  filtersActive?: number
}

const SidebarButton: React.FC<SidebarButtonProps> = ({ keyName, symbol, text, color, indent, showHide, selectBy, visible = true, classTitle, isExpanded, filtersActive }) => {
  const theme = useTheme();
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
    if (showHide) {
      event.stopPropagation() // only stop propagation if event is defined
      showHide({
        text: text,
        classTitle: classTitle,
      })
    }

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
      width={((selectBy === undefined) ? "100%" : "auto")}
      onClick={showHideFn}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      justifyContent="flex-start" variant='ghost' size='sm' ml={indent}>
      <Flex justify="space-between" wrap="wrap" width="100%" alignItems="center">
        <Box opacity={opacity}>
          <Icon h={3} as={IconMap[symbol] as any} color={color} mr={2} />
          {text}
          {(filtersActive! > 0) ?
            <Icon
              height="7px"
              mb="6px"
              ml="2px"
              color={theme.colors.ch_blue}
              variant="ghost" as={IconMap.circle as any} />
            : null}
        </Box>
        {(selectBy !== undefined) ?
          <Flex>
            <Icon
              _hover={{ backgroundColor: "rgba(0,0,0,0)" }}
              _active={{ backgroundColor: "rgba(0,0,0,0)" }}
              onClick={selectByFn}
              height="100%"
              opacity={eyeButtonOpacity}
              variant="ghost" aria-label='ShowHide' as={IconMap.select as any} />
          </Flex>
          : null}

        {(isExpanded !== undefined) ?
          <Flex>

            <Icon
              height="24px"
              variant="ghost" as={(isExpanded ? IconMap.open : IconMap.closed) as any} />
          </Flex>
          : null}
      </Flex>
    </Button>
  )
}

export default SidebarButton
