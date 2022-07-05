import { Flex } from '@chakra-ui/react';
import { Link, useParams } from 'react-router-dom';

export default function Datasets() {
  let params = useParams();

  return (
    <Flex mt="48px">
      Datasets
    </Flex>
  )
}