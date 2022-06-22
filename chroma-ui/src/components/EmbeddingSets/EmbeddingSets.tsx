import { Link } from 'react-router-dom';
import { useQuery, useSubscription } from 'urql';
import { useGetEmbeddingSetsQuery } from '../../graphql/graphql'

export default function EmbeddingSets() {
    const [result, reexecuteQuery] = useGetEmbeddingSetsQuery()
    
    const { data, fetching, error } = result;

    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    console.log('data', data)

    return (
    <>
      <ul>
          <h1>Embedding Sets</h1>
          {data?.embeddingSets.map(embeddingSet => (
            <>
              <Link to={embeddingSet.id} key={embeddingSet.id}>id: {embeddingSet.id}</Link>
            </>
          ))}
      </ul>
    </>
    )
}