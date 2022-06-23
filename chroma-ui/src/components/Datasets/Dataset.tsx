import { Link, useParams } from 'react-router-dom';
import { Container, Flex, Text, Box } from '@chakra-ui/react';
import { useQuery } from 'urql';
import SimpleList from '../Shared/SimpleList';

const DatasetQuery = `
  query getDataset($id: ID!) {
    dataset(id: $id) {
      id
      name
      datapoints {
        id
      }
      slices {
        id
        name
      }
      project {
        id
      }
    }
  }
`;

export default function Dataset() {
    let params = useParams();
    const [result, reexecuteQuery] = useQuery({query: DatasetQuery, variables: {id: params.dataset_id!}})

    const { data, fetching, error } = result;
    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    return (
      <Box mt="68px">
        <Link to={"/projects/" + data.dataset.project.id}>&larr; Back to project</Link>
        <Text mt={3} fontSize="xl">Dataset: {data?.dataset.name}</Text>
        <SimpleList data={data!.dataset.slices} headerName="Slices" displayName="name" pathBase="slices"/>
        <SimpleList data={data!.dataset.datapoints} headerName="Datapoints" displayName="id" pathBase=""/>
      </Box>
    )
}