import { useColorModeValue, Text, Box, GridItem, Grid as ChakraGrid, Center, Skeleton, TableContainer, Table, Tbody, Tr, Td, Flex, useTheme } from "@chakra-ui/react"
import { BiCategoryAlt } from "react-icons/bi"
import { BsLayers, BsTag } from "react-icons/bs"
import { datapointsAtom, datasetsAtom, labelsAtom, resourcesAtom, visibleDatapointsAtom } from "./atoms"
import Tags from "./Tags"
import { useAtom } from 'jotai';
import ImageRenderer from "./ImageRenderer"

interface DataPanelGridProps {
  datapointId: any
}

const DataPanelModal: React.FC<DataPanelGridProps> = ({ datapointId }) => {
  if (datapointId === undefined) return <></> // handle this case though we dont expect to run into it
  const [datapoints] = useAtom(datapointsAtom)
  const [resources] = useAtom(resourcesAtom)
  const [datasets] = useAtom(datasetsAtom)
  const [labels] = useAtom(labelsAtom)
  const datapoint = datapoints[datapointId]

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
            <ImageRenderer imageUri={resources[datapoint.resource].uri} annotations={datapoint.annotations} />
          </Flex>
        </GridItem>
        <GridItem colSpan={1} rowSpan={8}>
          <Text fontWeight={600} pb={2}>Data</Text>
          <TableContainer>
            <Table variant='simple' size="sm">
              <Tbody>
                <Tr key={"dpid"}>
                  <Td width="30%" fontSize="xs">Datapoint ID</Td>
                  <Td p={0} fontSize="xs">{datapoint.id}</Td>
                </Tr>
                <Tr key={"dataset"}>
                  <Td width="30%" fontSize="xs">Dataset</Td>
                  <Td p={0} fontSize="xs">{datasets[datapoint.dataset].name}</Td>
                </Tr>
                <Tr key={"dataset"}>
                  <Td width="30%" fontSize="xs">Inference</Td>
                  <Td p={0} fontSize="xs">{datapoint.inference}</Td>
                </Tr>
                {/* <Tr key={"quality"}>
                  <Td width="30%" fontSize="xs">Quality</Td>
                  <Td p={0} fontSize="xs">{(Math.exp(-parseFloat(datapoint.metadata.distance_score)) * 100).toFixed(3)}</Td>
                </Tr> */}
              </Tbody>
            </Table>
          </TableContainer>
          <Flex pt={5} alignItems="center">
            <BiCategoryAlt color='#666' />
            <Text ml={1} fontWeight={600}>Label</Text>
          </Flex>
          {/* <TableContainer>
            <Table variant='simple' size="sm">
              <Tbody>
                <Tr key={"category"}>
                  <Td width="30%" fontSize="xs">Category</Td>
                  <Td p={0} fontSize="xs">{labels[datapoint.label].data.categories[0].name}</Td>
                </Tr>
              </Tbody>
            </Table>
          </TableContainer> */}

          {/* <Flex pt={5} alignItems="center">
            <BsLayers color='#666' style={{ transform: "rotate(-90deg)" }} />
            <Text ml={1} fontWeight={600}>Inference</Text>
          </Flex>
          <TableContainer>
            <Table variant='simple' size="sm">
              <Tbody>
                <Tr key={"category"}>
                  <Td width="30%" fontSize="xs">Category</Td>
                  <Td p={0} fontSize="xs">{datapoint.inference?.data.categories[0].name}</Td>
                </Tr>
              </Tbody>
            </Table>
          </TableContainer> */}

          <Flex pt={5} alignItems="center">
            <BsTag color='#666' />
            <Text ml={1} fontWeight={600}>Tags</Text>
          </Flex>
          <Flex mt={3}>
            <Tags datapointId={datapoint.id} />
          </Flex>
        </GridItem> 

      </ChakraGrid>
    </Box >
  )
}

export default DataPanelModal