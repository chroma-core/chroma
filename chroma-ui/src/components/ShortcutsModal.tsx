import React from 'react'
import { Text, Kbd, IconButton, Button, useDisclosure, Drawer, DrawerBody, DrawerContent, DrawerHeader, DrawerOverlay, DrawerCloseButton, DarkMode, Box, Container, Grid, GridItem, useTheme, Flex, Tag, TagLabel, TagRightIcon } from '@chakra-ui/react'
import { MdDarkMode, MdLightMode, MdSettings } from 'react-icons/md';
import { TbHandTwoFingers, TbKeyboard } from 'react-icons/tb';

function ShortcutsModal() {
    const { isOpen, onOpen, onClose } = useDisclosure()
    const theme = useTheme()

    return (
        <>
            <Button onClick={onOpen} variant="ghost" borderRadius={0} height="100%" >
                <TbKeyboard />
            </Button>
            <Drawer
                closeOnEsc={false}
                closeOnOverlayClick={false}
                placement={'bottom'}
                onClose={onClose}
                isOpen={isOpen}
            >
                <DrawerOverlay bg='none' />
                <DrawerContent
                    bg={theme.colors.ch_gray.black}
                >
                    <DrawerCloseButton />
                    <DrawerBody minH={"200px"}>
                        <Container width='5xl' maxW='auto'>
                            <Grid templateColumns='repeat(3, 1fr)' gap={12}>
                                <GridItem>
                                    <Text fontWeight={600} color='white' mb={3}>Movement</Text>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Pan</Text>
                                        <Kbd>2 fingers pan</Kbd>
                                    </Flex>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Pan</Text>
                                        <Kbd>2 fingers pan</Kbd>
                                    </Flex>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Zoom</Text>
                                        <Kbd>2 fingers pinch</Kbd>
                                    </Flex>
                                </GridItem>
                                <GridItem>
                                    <Text fontWeight={600} color='white' mb={3}>Tools</Text>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Select</Text>
                                        <Kbd>V</Kbd>
                                    </Flex>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Lasso</Text>
                                        <Kbd>L</Kbd>
                                    </Flex>
                                </GridItem>
                                <GridItem>
                                    <Text fontWeight={600} color='white' mb={3}>Select</Text>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Pan</Text>
                                        <Kbd>2 fingers pan</Kbd>
                                    </Flex>
                                    <Flex alignItems={'center'} justify={'space-between'}>
                                        <Text color="white">Zoom</Text>
                                        <Kbd>2 fingers pinch</Kbd>
                                    </Flex>
                                </GridItem>
                                {/* <GridItem>
                                    <Text color='white'>Selection</Text>
                                    <span>
                                        <Kbd>shift</Kbd> <span style={{ color: 'white' }}>+</span> <Kbd>H</Kbd>
                                    </span>
                                </GridItem>
                                <GridItem>
                                    <span>
                                        <Kbd>shift</Kbd> + <Kbd>H</Kbd>
                                    </span>
                                </GridItem> */}
                            </Grid>
                        </Container>
                    </DrawerBody>
                </DrawerContent>
            </Drawer>
        </>
    )
}

export default ShortcutsModal
