import * as CSS from 'csstype'
import { Flex, Button, useTheme, Icon, Box } from '@chakra-ui/react'
<<<<<<< HEAD
import { BsCircle, BsFillSquareFill, BsXLg, BsSquare } from 'react-icons/bs'
import { AiOutlineEye, AiOutlineEyeInvisible } from 'react-icons/ai'
import { useState } from 'react'
import { IconType } from 'react-icons'
=======
import { BsCircleFill, BsFillSquareFill, BsXLg, BsSquare } from 'react-icons/bs';
import { AiOutlineEye, AiOutlineEyeInvisible } from 'react-icons/ai';
import { useState } from "react";
import { IconType } from "react-icons";
>>>>>>> master

const IconMap: any = {
  circle: BsCircleFill,
  cross: BsXLg,
  square: BsFillSquareFill,
  square_outline: BsSquare,
  show: AiOutlineEye,
  hide: AiOutlineEyeInvisible,
}

interface SidebarButtonProps {
  symbol: 'square' | 'cross' | 'circle' | 'square_outline'
  text: string
  color: CSS.Property.Color
  indent: number
  onClick?: ({}) => void
  visible: boolean
  classTitle: string
}

const SidebarButton: React.FC<SidebarButtonProps> = ({
  symbol,
  text,
  color,
  indent,
  onClick,
  visible = true,
  classTitle,
}) => {
  var icon: string = visible === true ? 'show' : 'hide'
  var opacity: string = visible === true ? '100%' : '30%'

  function buttonClicked() {
    if (onClick)
      onClick({
        text: text,
        classTitle: classTitle,
      })
  }

  return (
    <Button justifyContent="flex-start" variant="ghost" size="sm" ml={indent} onClick={buttonClicked} opacity={opacity}>
      <Flex justify="space-between" wrap="wrap" width="100%">
        <Box>
          <Icon as={IconMap[symbol] as any} color={color} mr={2} />
          {text}
        </Box>
        {!visible ? <Icon as={IconMap[icon] as any} color="black" /> : null}
      </Flex>
    </Button>
  )
}

export default SidebarButton
