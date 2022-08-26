import React from 'react';
import {
  Flex,
  Text,
  useColorModeValue,
  useTheme,
  Divider,
  Stack,
  Skeleton,
  RangeSliderTrack,
  RangeSlider,
  RangeSliderFilledTrack,
  RangeSliderThumb,
  Tooltip,
  Box,
  Accordion,
  AccordionButton,
  AccordionItem,
  AccordionPanel
} from '@chakra-ui/react'
import SidebarButton from '../Shared/SidebarButton';
import FilterSidebarHeader from '../Shared/FilterSidebarHeader';
import { useAtom } from 'jotai';
import { context__categoryFilterAtom, contextObjectSwitcherAtom, context__datapointsAtom, context__datasetFilterAtom, DataType, globalCategoryFilterAtom, globalDatapointAtom, globalDatasetFilterAtom, globalMetadataFilterAtom, globalSelectedDatapointsAtom, globalTagFilterAtom, globalVisibleDatapointsAtom, context__metadataFiltersAtom, pointsToSelectAtom, selectedDatapointsAtom, context__tagFilterAtom, visibleDatapointsAtom } from './atoms';
import { FilterArray, FilterType } from './types';

interface FilterSidebarProps {
  showSkeleton: boolean
}

const FilterSidebar: React.FC<FilterSidebarProps> = ({ showSkeleton }) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  const [datapoints] = useAtom(globalDatapointAtom)
  const [selectedDatapoints, setselectedDatapoints] = useAtom(globalSelectedDatapointsAtom)
  const [visibleDatapoints, setvisibleDatapoints] = useAtom(globalVisibleDatapointsAtom)
  const [categoryFilter, updatecategoryFilter] = useAtom(globalCategoryFilterAtom)
  const [tagFilter, updatetagFilter] = useAtom(globalTagFilterAtom)
  const [datasetFilter, updatedatasetFilter] = useAtom(globalDatasetFilterAtom)
  const [metadataFilters, updateMetadataFilter] = useAtom(globalMetadataFilterAtom)
  const [pointsToSelect, updatepointsToSelect] = useAtom(pointsToSelectAtom)
  const [contextObjectSwitcher, updatecontextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)

  const updateCategory = (data: any, fn: any) => {
    updatecategoryFilter(fn)
  }
  const updateTag = (data: any, fn: any) => {
    updatetagFilter(fn)
  }
  const updateDataset = (data: any, fn: any) => {
    updatedatasetFilter(fn)
  }
  const updateMetadata = (data: any, fn: any) => {
    // let findMatchedFilter = metatadataFilterMap.find(f => f.filter.name === data.filter.filter.name)
    // this is a bit of a hack
    updateMetadataFilter({ ...metadataFilters })
  }

  var metatadataFilterMap = Object.values(metadataFilters).map(m => {
    return { filter: m, update: updateMetadata }
  })

  const filterArray: FilterArray[] = [
    { filter: categoryFilter!, update: updateCategory },
    { filter: datasetFilter!, update: updateDataset },
    { filter: tagFilter!, update: updateTag },
    ...metatadataFilterMap
  ]
  // if (contextObjectSwitcher == DataType.Context) filterArray.push({ filter: tagFilter!, update: updateTag })

  function updateDiscreteFilter(passedFilter: any, passedOption: any) {
    let filterIndex = filterArray.findIndex(f => f.filter.name === passedFilter.name)
    var options = filterArray[filterIndex].filter.options!.slice()
    let optionIndex = filterArray[filterIndex].filter.options!.findIndex((option: any) => option.id === passedOption.id)

    options[optionIndex].visible = !options[optionIndex].visible
    filterArray[filterIndex].update({ 'filter': filterArray[filterIndex] }, (prev: any) => {
      return ({ ...prev, options: [...options] })
    })
  }

  function updateContinuousFilter(passedFilter: any, minVisible: number, maxVisible: number) {
    let findMatchedFilter = metatadataFilterMap.find(f => f.filter.name === passedFilter.name)
    findMatchedFilter!.filter.range.minVisible = minVisible
    findMatchedFilter!.filter.range.maxVisible = maxVisible
    updateMetadataFilter({ ...metadataFilters })
  }

  function selectPoints(dps: number[]) {
    updatepointsToSelect(dps)
    setselectedDatapoints(dps)
  }

  return (
    <Flex
      direction="column"
      minWidth={300}
      bg={bgColor}
      borderRight="1px"
      borderLeft="1px"
      borderColor={borderColor}
      maxHeight="100vh"
      overflowX="hidden"
      overflowY="scroll"
      css={{
        '&::-webkit-scrollbar': {
          width: '0px',
        },
      }}
      pt={14}>
      {!showSkeleton ?
        <Flex key="buttons" px={3} justifyContent="space-between" alignContent="center">
          <Text fontWeight={600}>Filter</Text>
          <Text fontSize="sm">{visibleDatapoints.length} / {Object.values(datapoints).length} total</Text>
        </Flex>
        : null}
      <Divider w="100%" pt={2} />
      <Flex direction="column" mt={2}>
        <Accordion defaultIndex={[0, 1, 2, 3, 4, 5, 6]} allowMultiple borderWidth={0}>
          {showSkeleton ?
            <Stack mx={3}>
              <Skeleton height='25px' />
              <Skeleton height='25px' style={{ marginLeft: '30px' }} />
              <Skeleton height='25px' style={{ marginLeft: '30px' }} />
              <Skeleton height='25px' />
              <Skeleton height='25px' style={{ marginLeft: '30px' }} />
              <Skeleton height='25px' style={{ marginLeft: '30px' }} />
            </Stack>
            : filterArray.map(function (f, index) {
              if (!f.filter) return
              let filtersActive = 0
              if (f.filter.type == FilterType.Discrete) {
                filtersActive = f.filter.options!.filter((o: any) => !o.visible).length
              } else if (f.filter.type == FilterType.Continuous) {
                filtersActive = 0
              }

              return (
                <AccordionItem w="100%" borderWidth={0} borderColor="rgba(0,0,0,0)" key={f.filter.name}>
                  {({ isExpanded }) => (
                    <React.Fragment key={index}>
                      <AccordionButton w="100%" p={0} m={0}>
                        <FilterSidebarHeader
                          text={f.filter.name}
                          symbol="square"
                          visible={true}
                          color="#f0f0f0"
                          indent={0}
                          classTitle={f.filter.name}
                          keyName={f.filter.name}
                          key={f.filter.name}
                          isExpanded={isExpanded}
                          filtersActive={filtersActive}
                        ></FilterSidebarHeader>
                      </AccordionButton>
                      <AccordionPanel p={0} m={0}>
                        <Flex direction="column">
                          {(f.filter.type == FilterType.Discrete) ?
                            f.filter.options!.map(function (option: any) {

                              var link = f.filter.linkedAtom[option.id]
                              return (
                                <SidebarButton
                                  text={link.name}
                                  visible={option.visible}
                                  symbol="circle"
                                  color={option.color}
                                  classTitle={f.filter.name}
                                  key={f.filter.name + "." + link.name}
                                  indent={6}
                                  keyName={f.filter.name + "." + link.name}
                                  showHide={() => updateDiscreteFilter(f.filter, option)}
                                  selectBy={() => selectPoints(link.datapoint_ids)}// selectByFilter(filter, option)}
                                />
                              )
                            })
                            : null}

                          {(f.filter.type == FilterType.Continuous) ?
                            <SliderThumbWithTooltip
                              min={f.filter.range!.min}
                              max={f.filter.range!.max}
                              minVisible={f.filter.range!.minVisible}
                              maxVisible={f.filter.range!.maxVisible}
                              update={updateContinuousFilter}
                              filter={f.filter}
                            />
                            : null}
                        </Flex>
                      </AccordionPanel>
                    </React.Fragment>
                  )}
                </AccordionItem>
              )
            })
          }
        </Accordion>
      </Flex>
    </Flex >
  )
}

export default FilterSidebar

interface SliderProps {
  min: number
  max: number
  minVisible: number
  maxVisible: number
  update: (filter: any, minVisible: number, maxVisible: number) => void
  filter: any
}

const SliderThumbWithTooltip: React.FC<SliderProps> = ({ min, max, minVisible, maxVisible, update, filter }) => {
  const [sliderValue, setSliderValue] = React.useState([min, max])
  const [showTooltip, setShowTooltip] = React.useState(false)

  const midValue = (max - min) / 2
  const step = (max - min) / 100

  function onEnd(val: number[]) {
    update(filter, sliderValue[0], sliderValue[1])
  }

  return (
    <Box px={10} py={3}>
      <RangeSlider
        aria-label={['min', 'max']}
        defaultValue={sliderValue}
        min={min}
        max={max}
        step={step}
        colorScheme='blue'
        onChange={(v) => setSliderValue(v)}
        onMouseEnter={() => setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
        onChangeEnd={(val) => onEnd(val)}
      >
        <RangeSliderTrack>
          <RangeSliderFilledTrack />
        </RangeSliderTrack>
        <Tooltip
          hasArrow
          bg='blue.500'
          color='white'
          placement='bottom'
          isOpen={showTooltip}
          label={`${sliderValue[0].toFixed(3)}`}
        >
          <RangeSliderThumb index={0} />
        </Tooltip>
        <Tooltip
          hasArrow
          bg='blue.500'
          color='white'
          placement='bottom'
          isOpen={showTooltip}
          label={`${sliderValue[1].toFixed(3)}`}
        >
          <RangeSliderThumb index={1} />
        </Tooltip>
      </RangeSlider>
      <Flex justifyContent="space-between">
        <Text fontSize="sm" fontWeight={600} opacity="50%">{min.toFixed(3)}</Text>
        <Text fontSize="sm" fontWeight={600} opacity="50%">{midValue.toFixed(3)}</Text>
        <Text fontSize="sm" fontWeight={600} opacity="50%">{max.toFixed(3)}</Text>
      </Flex>
    </Box>
  )
}