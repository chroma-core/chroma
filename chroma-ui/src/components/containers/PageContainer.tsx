import React, { useEffect } from 'react'
import { Flex } from '@chakra-ui/react'

interface PageContainerProps {
  children: React.ReactNode
  includeMessages?: boolean
}

const PageContainer: React.FC<PageContainerProps> = ({ children }) => {

  return (
    <>
      <Flex minHeight="100vh">
        {children}
      </Flex>
    </>
  )
}

export default PageContainer