import React, { useCallback, useEffect } from 'react'
import { Flex, Button, useTheme, Tooltip, useColorModeValue, IconButton, Container } from '@chakra-ui/react'
import { BsCursorFill, BsBoundingBox } from 'react-icons/bs'
import { TbLasso } from 'react-icons/tb'
import { Link, useLocation, useParams } from 'react-router-dom'

export default function Header() {

  return (
    <Flex
      as="header"
      position="fixed"
      w="100%"
      height="48px"
      zIndex={10}
      p={0}
      justifyContent="space-between"
    >
      <Container maxWidth={1200}>
        <Link to={"/"}>
          <Button
            height="100%"
            variant='solid'
            color="blue"
          >
            Model Spaces
          </Button>
        </Link>
      </Container>
    </Flex>
  )
}
