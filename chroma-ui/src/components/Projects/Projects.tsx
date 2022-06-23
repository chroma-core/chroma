import { Container, Flex, Text, Box, useTheme} from '@chakra-ui/react';
import { Link, useParams } from 'react-router-dom';
import { useGetProjectsQuery } from '../../graphql/graphql';
import SimpleList from '../Shared/SimpleList';
// import { useGetPro } from '../graphql/graphql'

export default function Projects() {
    let params = useParams();
    const theme = useTheme()

    const [result, reexecuteQuery] = useGetProjectsQuery()
    const { data, fetching, error } = result;
    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    let noData = (data?.projects.length == 0)

    return (
      <SimpleList data={data?.projects} headerName="Projects" displayName="name" pathBase="projects"/>
    )
}