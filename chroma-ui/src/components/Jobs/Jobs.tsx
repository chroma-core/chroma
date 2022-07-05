import { Link, useParams } from 'react-router-dom';
import { Container, Flex, Text, Box, useTheme} from '@chakra-ui/react';
import { useGetJobsQuery } from '../../graphql/graphql';
import SimpleList from '../Shared/SimpleList';

export default function Jobs() {
    let params = useParams();
    const theme = useTheme()

    const [result, reexecuteQuery] = useGetJobsQuery()
    const { data, fetching, error } = result;
    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    let noData = (data?.jobs.length == 0)
    
    return (
      <SimpleList data={data?.jobs} headerName="Jobs" displayName="id" pathBase="jobs"/>
    )
}