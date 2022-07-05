import React, { useEffect } from 'react'
import { Flex, Box, Container } from '@chakra-ui/react'
import { Link, useParams } from 'react-router-dom';
import AppHeader from '../AppHeader';

interface AppContainerProps {
  children?: React.ReactNode
  includeMessages?: boolean
}

export default function AppContainer({ children, includeMessages }: AppContainerProps) {
  let params = useParams();

  return (
    <Flex>
      <AppHeader />
      <Container mt="48px">
        {children}
      </Container>
    </Flex>
  )
}