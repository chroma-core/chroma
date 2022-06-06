import React, { useEffect } from 'react'
import { Flex } from '@chakra-ui/react'
import { Helmet } from 'react-helmet'

interface PageContainerProps {
  children: React.ReactNode
  includeMessages?: boolean
}

const PageContainer: React.FC<PageContainerProps> = ({ children }) => {

  return (
    <>
      <Helmet defaultTitle="Chroma" />
      <Flex minHeight="100vh">
        {children}
      </Flex>
    </>
  )
}

export default PageContainer