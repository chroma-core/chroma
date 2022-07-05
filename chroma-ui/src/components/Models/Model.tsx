import { Link, useParams } from 'react-router-dom';
import { Container, Flex, Text, Box } from '@chakra-ui/react';
import { useQuery } from 'urql';
import SimpleList from '../Shared/SimpleList';

const ModelQuery = `
  query getModelArchitecture($id: ID!) {
    modelArchitecture(id: $id) {
      id
      name
      trainedModels {
        id
      }
    }
  }
`;

export default function Model() {
    let params = useParams();
    const [result, reexecuteQuery] = useQuery({query: ModelQuery, variables: {id: params.model_id!}})

    const { data, fetching, error } = result;
    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    return (
      <Box mt="68px">
        <Text fontSize="xl">Model Architecture: {data?.modelArchitecture.name}</Text>
        <SimpleList data={data!.modelArchitecture.trainedModels} headerName="Trained Models" displayName="id" pathBase="trainedmodels"/>
      </Box>
    )
}