import { atomWithQuery } from 'jotai/urql'
import { client } from '../index'

const projectsAtom = atomWithQuery(
    (get) => ({
        query: '{ projects { id name } }',
    }),
    () => client
)

export { projectsAtom }
