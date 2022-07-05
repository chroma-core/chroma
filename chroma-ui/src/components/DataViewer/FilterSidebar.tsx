import React from 'react';
import { Flex, Text, Box, Button, useColorModeValue, useTheme, Divider, Square, Icon, Tabs, TabList, Tab, Stack, Skeleton } from '@chakra-ui/react'
import { BsFillSquareFill } from 'react-icons/bs';
import SidebarButton from '../Shared/SidebarButton';

interface FilterSidebarProps {
    filters: any[]
    setFilters: (filters: any) => void
    selectByFilter: (filter: any, option: any) => void
    showSkeleton: boolean
    numVisible: number
    numTotal: number
}

const FilterSidebar: React.FC<FilterSidebarProps> = ({ filters, setFilters, showSkeleton, numVisible, numTotal, selectByFilter }) => {
    const theme = useTheme();
    const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
    const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

    function updateFilter(passedFilter: any, passedOption: any) {
        let filterIndex = filters.findIndex(filter => filter.name === passedFilter.name)
        let optionIndex = filters[filterIndex].optionsSet.findIndex((option: any) => option.name === passedOption.name)
        filters[filterIndex].optionsSet[optionIndex].visible = !filters[filterIndex].optionsSet[optionIndex].visible
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
                        return (
                            <React.Fragment key={index}>
                                <SidebarButton
                                    text={filter.name}
                                    symbol="square"
                                    visible={true}
                                    color="#f0f0f0"
                                    indent={0}
                                    classTitle={filter.name}
                                    keyName={filter.name}
                                    key={filter.name}
                                ></SidebarButton>
                                {(filter.type == 'discrete') ?
                                    filter.optionsSet.map(function (option: any) {

                                        return (
                                            <SidebarButton
                                                text={option.name}
                                                visible={option.visible}
                                                symbol="circle"
                                                color={option.color}
                                                classTitle={filter.title}
                                                key={option.name}
                                                indent={6}
                                                keyName={option.name}
                                                onClick={() => updateFilter(filter, option)}
                                                onClick2={() => selectByFilter(filter, option)}

                                            />
                                        )
                                    })
                                    : null}

                            </React.Fragment>
                        )
                    })
                }
            </Flex>
        </Flex >
    )
}

export default FilterSidebar
