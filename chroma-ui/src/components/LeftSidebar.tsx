// @ts-nocheck
import React from 'react';
import { Flex, Center, Box, Button, useColorModeValue, useTheme, Divider, Square, Icon } from '@chakra-ui/react'
import { BsFillSquareFill } from 'react-icons/bs';
import SidebarButton from './Shared/SidebarButton';

interface LeftSidebarProps {
  classClicked: ((classtring: string) => void)
  typeClicked: ((typestring: string) => void)
  classDict: any[]
}

const LeftSidebar: React.FC<LeftSidebarProps> = ({ classClicked, typeClicked, classDict }) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#F3F5F6",  '#0c0c0b')

  if (classDict === undefined) {
    classDict = [{
      title: 'no data',
      color: 'red',
      visible: true,
      subtypes: []
    }]
  }

  return (
      <Flex 
        direction="column" 
        minWidth={300} 
        bg={bgColor} 
        borderRight="1px"
        borderLeft="1px"
        borderColor={theme.colors.ch_gray.dark}
        p={3}
        maxHeight="100vh"
        overflowX="hidden"
        overflowY="scroll"
        css={{
          '&::-webkit-scrollbar': {
            width: '0px',
          },
        }}
        pt={16}>
        <Flex key="buttons">
            <Button variant='ghost' size='sm' disabled>Classes</Button>
            {/* <Button variant='ghost' size='sm'>Filter</Button> */}
        </Flex>
        <Divider w="100%" pt={2}/>
        <Flex direction="column" mt={2}>
          {classDict.map(function(chClass, index){
            return (
              <React.Fragment key={index}>
                <SidebarButton 
                text={chClass.title}
                symbol="square" 
                visible={chClass.visible}
                color={chClass.color}
                indent={0}
                classTitle={chClass.title}
                keyName={chClass.title}
                onClick={classClicked}
              ></SidebarButton>
              
              {chClass.subtypes.map(function(chType){
                return (
                <SidebarButton 
                text={chType.title}
                visible={chType.visible}
                symbol="circle" 
                color={chType.color} 
                classTitle={chClass.title}
                key={chType.title}
                indent={6}
                onClick={typeClicked}>
                </SidebarButton>
                )
              })}
            </React.Fragment>
          )
        })}
      </Flex>
    </Flex>
  );
}

export default LeftSidebar;
