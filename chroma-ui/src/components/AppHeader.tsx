import React, { useCallback, useEffect } from 'react'
import { Flex, Button, useTheme, Tooltip, useColorModeValue, IconButton, Container } from '@chakra-ui/react'
import { BsCursorFill, BsBoundingBox } from 'react-icons/bs'
import { TbLasso } from 'react-icons/tb'
import { Link, useLocation, useParams } from 'react-router-dom'

export enum HeaderRouteOptions {
  PROJECT = "project",
  JOB = "job",
}

export default function Header() {
  const params = useParams()
  let location = useLocation()
  const theme = useTheme()

  const [pathSelected, setPathSelected] = React.useState<HeaderRouteOptions>(HeaderRouteOptions.PROJECT);
  React.useEffect(() => {
    if (location.pathname.startsWith("/jobs")) {
      setPathSelected(HeaderRouteOptions.JOB)
    } else {
      setPathSelected(HeaderRouteOptions.PROJECT)
    }
  }, [location.pathname]);

  return (
    <Flex
      as="header"
      position="fixed"
      w="100%"
      bg={theme.colors.ch_gray.black}
      height="48px"
      borderBottom="1px"
      borderColor={theme.colors.ch_gray.dark}
      zIndex={10}
      p={0}
      justifyContent="space-between"
    >
      <Container maxWidth={1200}>
        <Link to={"/"}>
          <Button
            borderRadius={0}
            height="100%"
            pr={4}
            pl={4}
            variant='ghost'
            color="white"
            borderBottomStyle="solid"
            borderBottom="4px solid"
            borderColor={(pathSelected == HeaderRouteOptions.PROJECT) ? theme.colors.ch_blue : "rgba(0,0,0,0)"}
            _hover={{ bg: theme.colors.ch_gray.black, color: "white" }}
            _active={{ bg: theme.colors.ch_gray.black, color: "white" }}
          >
            Projects
          </Button>
        </Link>
        {/* <Link to={"/jobs"}>
        <Button
            borderRadius={0}
            height="100%"
            pr={4}
            pl={4}
            variant='ghost'
            color="white"
            borderBottomStyle="solid"
            borderBottom="4px solid"
            borderColor={(pathSelected==HeaderRouteOptions.JOB) ? theme.colors.ch_blue : "rgba(0,0,0,0)"}
            _hover={{ bg: theme.colors.ch_gray.black, color: "white" }}
            _active={{ bg: theme.colors.ch_gray.black, color: "white" }}
          >
            Jobs
        </Button>
        </Link> */}
      </Container>
    </Flex>
  )
}
