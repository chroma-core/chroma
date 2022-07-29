import { Container, Flex, Text, Box, useTheme } from '@chakra-ui/react';
import { Link, useParams } from 'react-router-dom';
import SimpleList from '../Shared/SimpleList';
import { useAtom } from 'jotai'
import { projectsAtom } from '../../atoms/projectAtom'
import { Suspense } from 'react';

export default function Projects() {
  let params = useParams();
  const theme = useTheme()
  const [{ data, error }] = useAtom(projectsAtom)
  if (error) return <p>Oh no... {error.message}</p>;
  let noData = (data?.projects.length == 0)

  return (
    <>
      <Suspense fallback="Loading...">
        <SimpleList data={data?.projects} headerName="Projects" displayName="name" pathBase="data_viewer" />
      </Suspense>
    </>
  )
}