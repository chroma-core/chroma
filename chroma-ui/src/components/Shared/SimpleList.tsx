import { Container, Flex, Text, Box, useTheme} from '@chakra-ui/react';
import { Link, useParams } from 'react-router-dom';
import { useGetProjectsQuery } from '../../graphql/graphql';
// import { useGetPro } from '../graphql/graphql'

interface SimpleListProps {
    data: any[] | undefined
    displayName: string
    pathBase: String
    headerName: String
  }

export default function SimpleList({data, displayName = "id", pathBase, headerName}: SimpleListProps) {
    const theme = useTheme()

    let noData = ((data?.length == 0) || (data === undefined))

    return (
      <Container mt={6} width="5xl">
        <Text fontSize="xl" fontWeight={600}>{headerName}</Text>
        <Box border="1px solid #ccc" width="100%" borderRadius={4}>
        {data?.map(item => (
          <Link to={pathBase + "/" + item.id} key={item.id}>
            <Box py={3} px={5} borderBottom="1px solid #ccc" fontWeight={600} color={theme.colors.ch_blue}>{item[displayName]}</Box>
          </Link>
        ))}
        {noData? 
          <Box py={3} px={5} borderBottom="1px solid #ccc" fontWeight={600}>Nothing yet</Box>
        : null}
        </Box>
      </Container>
    )
}