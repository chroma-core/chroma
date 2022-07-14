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
  Slider,
  SliderFilledTrack,
  SliderMark,
  SliderThumb,
  SliderTrack,
  Tooltip,
  Box,
  Accordion,
  AccordionButton,
  AccordionItem,
  AccordionPanel
} from '@chakra-ui/react'
import SidebarButton from '../Shared/SidebarButton';
import FilterSidebarHeader from '../Shared/FilterSidebarHeader';

interface FilterSidebarProps {
  filters: any[]
  setFilters: (filters: any) => void
  selectByFilter: (filter: any, option: any) => void
  showSkeleton: boolean
  numVisible: number
  numTotal: number
}

const FilterSidebar: React.FC<FilterSidebarProps> = ({
  filters,
  setFilters,
  showSkeleton,
  numVisible,
  numTotal,
  selectByFilter }
) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  function updateDiscreteFilter(passedFilter: any, passedOption: any) {
    let filterIndex = filters.findIndex(filter => filter.name === passedFilter.name)
    let optionIndex = filters[filterIndex].optionsSet.findIndex((option: any) => option.name === passedOption.name)
    filters[filterIndex].optionsSet[optionIndex].visible = !filters[filterIndex].optionsSet[optionIndex].visible
    setFilters([...filters])
  }

  function updateContinuousFilter(passedFilter: any, minVisible: number, maxVisible: number) {
    let filterIndex = filters.findIndex(filter => filter.name === passedFilter.name)
    filters[filterIndex].optionsSet.minVisible = minVisible
    filters[filterIndex].optionsSet.maxVisible = maxVisible
    setFilters([...filters])
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
          <Text fontSize="sm">{numVisible} / {numTotal} total</Text>
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
            : filters.map(function (filter, index) {
              let filtersActive = 0
              if (filter.type == 'discrete') {
                filtersActive = filter.optionsSet.filter((o: any) => !o.visible).length
              } else if (filter.type == 'continuous') {
                filtersActive = 0
              }

              return (
                <AccordionItem w="100%" borderWidth={0} borderColor="rgba(0,0,0,0)" key={filter.name}>
                  {({ isExpanded }) => (
                    <React.Fragment key={index}>
                      <AccordionButton w="100%" p={0} m={0}>
                        <FilterSidebarHeader
                          text={filter.name}
                          symbol="square"
                          visible={true}
                          color="#f0f0f0"
                          indent={0}
                          classTitle={filter.name}
                          keyName={filter.name}
                          key={filter.name}
                          isExpanded={isExpanded}
                          filtersActive={filtersActive}
                        ></FilterSidebarHeader>
                      </AccordionButton>
                      <AccordionPanel p={0} m={0}>
                        <Flex direction="column">
                          {(filter.type == 'discrete') ?
                            filter.optionsSet.map(function (option: any) {
                              return (
                                <SidebarButton
                                  text={option.name}
                                  visible={option.visible}
                                  symbol="circle"
                                  color={option.color}
                                  classTitle={filter.title}
                                  key={filter.name + "." + option.name}
                                  indent={6}
                                  keyName={option.name}
                                  showHide={() => updateDiscreteFilter(filter, option)}
                                  selectBy={() => selectByFilter(filter, option)}
                                />
                              )
                            })
                            : null}

                          {(filter.type == 'continuous') ?
                            <SliderThumbWithTooltip
                              min={filter.optionsSet.min}
                              max={filter.optionsSet.max}
                              minVisible={filter.optionsSet.minVisible}
                              maxVisible={filter.optionsSet.maxVisible}
                              update={updateContinuousFilter}
                              filter={filter}
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