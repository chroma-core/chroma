import React from 'react'
import { Text, IconButton, Button, useDisclosure, Drawer, DrawerBody, DrawerContent, DrawerHeader, DrawerOverlay, DrawerCloseButton, DarkMode, Box, Container, Grid, GridItem, useTheme, Flex, Tag, TagLabel, TagRightIcon, Divider, Tab, TabList, TabPanel, TabPanels, Tabs, Icon } from '@chakra-ui/react'
import { MdDarkMode, MdLightMode, MdSettings } from 'react-icons/md';
import { TbHandTwoFingers, TbKeyboard, TbLasso } from 'react-icons/tb';
import { BsCursorFill } from 'react-icons/bs';

function ShortcutsDrawer() {
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
                variant="alwaysOpen"
            >
                <DrawerOverlay bg='none' style={{ pointerEvents: 'none' }} />
                <DrawerContent
                    bg="#222"
                >
                    <DrawerCloseButton _focus={{ outline: 'none' }} color="white" />
                    <DrawerBody minH={"200px"} p={0}>
                        <Tabs color="white" variant="unstyled">
                            <Container width='4xl' maxW='auto'>
                                <TabList>
                                    <Tab style={{ border: "2px solid rgba(0,0,0,0)" }} _selected={{ borderBottom: "2px solid #fff !important" }} _focus={{ outline: 'none' }} _active={{ bg: 'black' }}>Select</Tab>
                                    <Tab style={{ border: "2px solid rgba(0,0,0,0)" }} _selected={{ borderBottom: "2px solid #fff !important" }} _focus={{ outline: 'none' }} _active={{ bg: 'black' }}>Camera</Tab>
                                    <Tab style={{ border: "2px solid rgba(0,0,0,0)" }} _selected={{ borderBottom: "2px solid #fff !important" }} _focus={{ outline: 'none' }} _active={{ bg: 'black' }}>Tools</Tab>
                                </TabList>
                            </Container>
                            <Divider borderBottomColor="#4b4b46" />
                            <Container width='4xl' maxW='auto' pt={2}>
                                <TabPanels>
                                    <TabPanel pt={2}>
                                        <Grid templateColumns='repeat(3, 1fr)' gap={12}>
                                            <GridItem>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Select</Text>
                                                    <Text><Tag variant="darkMode">Click</Tag></Text>
                                                </Flex>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Deselect all</Text>
                                                    <Text><Tag variant="darkMode">ESC</Tag></Text>
                                                </Flex>
                                            </GridItem>
                                        </Grid>
                                    </TabPanel>
                                    <TabPanel pt={2}>
                                        <Grid templateColumns='repeat(3, 1fr)' gap={12}>
                                            <GridItem>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Pan</Text>
                                                    <Text><Tag variant="darkMode">Scroll</Tag></Text>
                                                </Flex>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Pan</Text>
                                                    <Text><Tag variant="darkMode">Space</Tag> + <Tag variant="darkMode">Drag</Tag></Text>
                                                </Flex>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Pan</Text>
                                                    <Text><Tag variant="darkMode">Middle Click</Tag> + <Tag variant="darkMode">Drag</Tag></Text>
                                                </Flex>
                                            </GridItem>
                                            <GridItem>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Zoom</Text>
                                                    <Text><Tag variant="darkMode">Pinch</Tag></Text>
                                                </Flex>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Text>Zoom</Text>
                                                    <Text><Tag variant="darkMode">⌘</Tag> + <Tag variant="darkMode">Scroll</Tag></Text>
                                                </Flex>
                                            </GridItem>
                                        </Grid>
                                    </TabPanel>
                                    <TabPanel pt={2}>
                                        <Grid templateColumns='repeat(3, 1fr)' gap={12}>
                                            <GridItem>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Flex alignItems="center"><Icon h={3} as={BsCursorFill} mr={2} />Move</Flex>
                                                    <Text><Tag variant="darkMode">V</Tag></Text>
                                                </Flex>
                                                <Flex justifyContent="space-between" mb={3}>
                                                    <Flex alignItems="center"><Icon h={3} as={TbLasso} mr={2} />Lasso</Flex>
                                                    <Text><Tag variant="darkMode">L</Tag></Text>
                                                </Flex>
                                            </GridItem>
                                        </Grid>
                                    </TabPanel>
                                </TabPanels>
                            </Container>
                        </Tabs>
                    </DrawerBody>
                </DrawerContent>
            </Drawer >
        </>
    )
}

export default ShortcutsDrawer

