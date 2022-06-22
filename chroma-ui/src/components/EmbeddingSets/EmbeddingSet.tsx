// @ts-nocheck
import { Link, useParams } from 'react-router-dom';
import { useQuery, useSubscription } from 'urql';
import { useGetEmbeddingSetQuery } from '../../graphql/graphql'

const FetchEmbeddingSetandProjectionSets = `
query getEmbeddingSet($id: ID!) {
    embeddingSet(id: $id) {
      id
      projectionSets {
        id
      }
    }
  }
`;

export default function EmbeddingSets() {
    let params = useParams();

    const [result, reexecuteQuery] = useQuery({
        query: FetchEmbeddingSetandProjectionSets, 
        variables: {id: (params.embedding_set_id!).toString()}
    })
    
    const { data, fetching, error } = result;

    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    return (
    <>
      <ul>
          <Link to="/embedding_sets">All Embedding Sets</Link>
          <h1>Embedding Set</h1>
          {data?.embeddingSet.id}
          <h3>Projection Sets</h3>
          {data.embeddingSet.projectionSets.map(projectionSet => (
            <p><Link to={"/projection_set/" + projectionSet.id} key={projectionSet.id}>id: {projectionSet.id}</Link></p>
          ))}
      </ul>
    </>
    )
}