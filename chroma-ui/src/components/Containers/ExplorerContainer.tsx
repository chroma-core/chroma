import React, { useEffect } from 'react'
import { Flex } from '@chakra-ui/react'

interface ExplorerContainerProps {
  children: React.ReactNode
  includeMessages?: boolean
}

const ExplorerContainer: React.FC<ExplorerContainerProps> = ({ children }) => {
  return (
    <>
      <Flex minHeight="100vh">
        {children}
      </Flex>
    </>
  )
}

export default ExplorerContainer
