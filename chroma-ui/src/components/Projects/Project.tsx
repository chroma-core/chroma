import { Link, useParams } from 'react-router-dom';
import { Container, Flex, Text, Box } from '@chakra-ui/react';
import { useGetProjectQuery } from '../../graphql/graphql';
import { useQuery } from 'urql';
import SimpleList from '../Shared/SimpleList';

const ProjectQuery = `
  query getProject($id: ID!) {
    project(id: $id) {
      id
      name
      datasets {
        id
        name
      }
      modelArchitectures {
        id
        name
      }
    }
  }
`;

export default function Project() {
    let params = useParams();
    const [result, reexecuteQuery] = useQuery({query: ProjectQuery, variables: {id: params.project_id!}})

    const { data, fetching, error } = result;
    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    return (
      <Box mt="68px">
        <Text fontSize="xl">Project: {data?.project.name}</Text>
        <SimpleList data={data!.project.datasets} headerName="Datasets" displayName="name" pathBase="datasets"/>
        <SimpleList data={data!.project.modelArchitectures} headerName="Model Architectures" displayName="id" pathBase="models"/>
      </Box>
    )
}