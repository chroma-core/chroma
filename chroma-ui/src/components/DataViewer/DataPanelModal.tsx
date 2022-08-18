import { ButtonGroup, useColorModeValue, Text, Box, GridItem, Grid as ChakraGrid, Center, Skeleton, TableContainer, Table, Tbody, Tr, Td, Flex, useTheme, Button } from "@chakra-ui/react"
import { BiCategoryAlt } from "react-icons/bi"
import { BsLayers, BsTag } from "react-icons/bs"
import { categoriesAtom, contextObjectSwitcherAtom, datapointsAtom, datasetsAtom, DataType, globalDatapointAtom, globalResourcesAtom, inferencesAtom, labelsAtom, resourcesAtom, visibleDatapointsAtom } from "./atoms"
import Tags from "./Tags"
import { useAtom } from 'jotai';
import ImageRenderer from "./ImageRenderer"
import { useState } from "react"

interface DataPanelGridProps {
  datapointId: any
}

const DataPanelModal: React.FC<DataPanelGridProps> = ({ datapointId }) => {
  if (datapointId === undefined) return <></> // handle this case though we dont expect to run into it
  const [datapoints] = useAtom(globalDatapointAtom)
  const [resources] = useAtom(globalResourcesAtom)
  const [categories] = useAtom(categoriesAtom)
  const datapoint = datapoints[datapointId]
  const [contextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)

  enum AnnotationsViewed {
    Labels,
    Inferences,
  }

  const [labelsInferences, setLabelsInferences] = useState(AnnotationsViewed.Labels)

  let labelsToView = datapoint.annotations
  if (labelsInferences == AnnotationsViewed.Inferences) labelsToView = datapoint.inferences

  const theme = useTheme()
  const bgColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  return (
    <Box
      key={datapointId}
      width="100%"
      flexGrow={1}
    >
      <ChakraGrid templateColumns='repeat(3, 1fr)' gap={6} height="100%" py={3}>
        <GridItem colSpan={2} rowSpan={8} bgColor={bgColor}>
          <Flex direction="row" alignItems="center" justifyContent="center" height="100%">
            <ImageRenderer imageUri={resources[datapoint.resource_id].uri} annotations={labelsToView} />
            {((datapoint.inferences.length > 0) && (datapoint.annotations.length > 0)) ?
              <ButtonGroup pos="absolute" variant='outline' spacing='1' bottom="40px">
                <Button
                  color={(labelsInferences == AnnotationsViewed.Labels) ? 'white' : undefined}
                  _hover={(labelsInferences == AnnotationsViewed.Labels) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
                  _active={(labelsInferences == AnnotationsViewed.Labels) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
                  backgroundColor={(labelsInferences == AnnotationsViewed.Labels) ? theme.colors.ch_blue : null}
                  onClick={() => setLabelsInferences(AnnotationsViewed.Labels)}>Labels</Button>
                <Button
                  color={(labelsInferences == AnnotationsViewed.Inferences) ? 'white' : undefined}
                  _hover={(labelsInferences == AnnotationsViewed.Inferences) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
                  _active={(labelsInferences == AnnotationsViewed.Inferences) ? { backgroundColor: theme.colors.ch_gray.ch_blue, color: "white" } : undefined}
                  backgroundColor={(labelsInferences == AnnotationsViewed.Inferences) ? theme.colors.ch_blue : null}
                  onClick={() => setLabelsInferences(AnnotationsViewed.Inferences)}>Inferences</Button>
              </ButtonGroup>
              : null}
          </Flex>
        </GridItem>
        <GridItem colSpan={1} rowSpan={8}>
          <Box h="100%" position="absolute" w="25%" overflowX="scroll">
            <Text fontWeight={600} pb={2}>Data</Text>
            <TableContainer>
              <Table variant='simple' size="sm">
                <Tbody>
                  <Tr key={"dpid"}>
                    <Td width="30%" fontSize="xs">Datapoint ID</Td>
                    <Td p={0} fontSize="xs">{datapoint.id}</Td>
                  </Tr>
                  <Tr key={"dpid"}>
                    <Td width="30%" fontSize="xs">Resource URI</Td>
                    <Td p={0} fontSize="xs">{resources[datapoint.resource_id].uri}</Td>
                  </Tr>
                  {/* <Tr key={"dataset"}>
                  <Td width="30%" fontSize="xs">Dataset</Td>
                  <Td p={0} fontSize="xs">{datasets[datapoint.dataset_id].name}</Td>
                </Tr> */}
                  {/* <Tr key={"quality"}>
                  <Td width="30%" fontSize="xs">Quality</Td>
                  <Td p={0} fontSize="xs">{(Math.exp(-parseFloat(datapoint.metadata.distance_score)) * 100).toFixed(3)}</Td>
                </Tr> */}
                </Tbody>
              </Table>
            </TableContainer>

            <Flex pt={5} alignItems="center">
              <Text fontWeight={600}>Metadata</Text>
            </Flex>

            <TableContainer>
              <Table variant='simple' size="sm">
                <Tbody>
                  {Object.keys(datapoint.metadata).map((a: string) => {
                    // @ts-ignore
                    const val = datapoint.metadata[a]
                    return (
                      <Tr key={a}>
                        <Td width="30%" p={1} fontSize="xs">{a}</Td>
                        <Td p={1} fontSize="xs">{val}</Td>
                      </Tr>
                    )
                  })}
                </Tbody>
              </Table>
            </TableContainer>

            <Flex pt={5} alignItems="center">
              <BiCategoryAlt color='#666' />
              <Text ml={1} fontWeight={600}>Labels - {datapoint.annotations.length}</Text>
            </Flex>

            <TableContainer>
              <Table variant='simple' size="sm">
                <Tbody>
                  {datapoint.annotations.map(a => {
                    return (
                      <Tr key={a.id}>
                        <Td p={1} fontSize="xs">{categories[a.category_id].name}</Td>
                      </Tr>
                    )
                  })}
                  {(datapoint.annotations.length === 0) ?
                    <Tr p={1} fontSize="xs" key={"category"}>
                      None yet
                    </Tr>
                    : null}
                </Tbody>
              </Table>
            </TableContainer>

            <Flex pt={5} alignItems="center">
              <BsLayers color='#666' style={{ transform: "rotate(-90deg)" }} />
              <Text ml={1} fontWeight={600}>Inferences - {datapoint.inferences.length}</Text>
            </Flex>

            <TableContainer>
              <Table variant='simple' size="sm">
                <Tbody>
                  {datapoint.inferences.map(a => {
                    return (
                      <Tr key={a.id}>
                        {/* <Td width="30%" fontSize="xs">Category</Td> */}
                        <Td p={1} fontSize="xs">{categories[a.category_id].name}</Td>
                      </Tr>
                    )
                  })}
                  {(datapoint.inferences.length === 0) ?
                    <Tr p={1} fontSize="xs" key={"category"}>
                      None yet
                    </Tr>
                    : null}
                </Tbody>
              </Table>
            </TableContainer>

            {(contextObjectSwitcher == DataType.Context) ?
              <>
                <Flex pt={5} alignItems="center">
                  <BsTag color='#666' />
                  <Text ml={1} fontWeight={600}>Tags</Text>
                </Flex>
                <Flex mt={3}>
                  <Tags datapointId={datapoint.id} />
                </Flex>
              </>
              : null}
          </Box>
        </GridItem>

      </ChakraGrid>
    </Box >
  )
}

export default DataPanelModal
