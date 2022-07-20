import { Container, Flex, Text, Box, useTheme } from '@chakra-ui/react';
import { Link, useParams } from 'react-router-dom';
import { useQuery } from 'urql';
import { useGetProjectsQuery } from '../../graphql/graphql';
import SimpleList from '../Shared/SimpleList';

const GetProjectsAndProjectionSets = `
query getProjectionSet {
    projects {
      id
      name
    }
}
`

export default function Projects() {
  let params = useParams();
  const theme = useTheme()

  const [result, reexecuteQuery] = useQuery({
    query: GetProjectsAndProjectionSets,
  })
  const { data, fetching, error } = result;
  if (fetching) return <p>Loading...</p>;
  if (error) return <p>Oh no... {error.message}</p>;

  let noData = (data?.projects.length == 0)

  console.log('data', data)

  return (
    <>
      <SimpleList data={data?.projects} headerName="Projects" displayName="name" pathBase="data_viewer" />
    </>

  )
}