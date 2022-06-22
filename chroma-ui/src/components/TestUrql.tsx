// @ts-nocheck

import { useQuery, useSubscription } from 'urql';
import { useGetProjectionSetQuery } from '../graphql/graphql'

// const FetchEmbeddingsQuery = `
// query fetchEmbeddings {
//   embeddings {
//     id
//     name
//     projections {
//       id
//       name
//     }
//   }
//   datasets {
//     id
//     name
//   }
// }
// `;

// const newMessages = `
//   subscription MessageSub {
//     count
//   }
// `;

// const handleSubscription = (response, message) => {
//   console.log('response', response, message.count)
//   return message;
// };

export default function TestUqrl() {
    // const [result, reexecuteQuery] = useGetProjectionSetsQuery()
    const [result, reexecuteQuery] = useGetProjectionSetQuery({variables: {
      id: 2,
    }})
    
    // const [res] = useSubscription({ query: newMessages }, handleSubscription);

    const { data, fetching, error } = result;

    if (fetching) return <p>Loading...</p>;
    if (error) return <p>Oh no... {error.message}</p>;

    console.log('data', data)

    return (
    <>
      <ul>
          <h1>ProjectionSet / Projections in yellow</h1>
          {data.projectionSet.id}
          {data.projectionSet.projections.map(projection => (
            <>
              <li key={projection.id}>x: {projection.x}, y: {projection.y}</li>
              {/* {embedding.projections.map(projection => (
                  <li style={{backgroundColor: 'yellow'}} key={projection.id}>{projection.name}</li>
              ))} */}
            </>
          ))}
      </ul>
      {/* <h1>subscription - off</h1> */}
      <ul>
        {/* {res.data.count} */}
        {/* {res.data.map(message => (
          console.log('message', message)
          // <p key={message.id}>
          //   {message.from}: "{message.text}"
          // </p>
        ))} */}
      </ul>
    </>
    );
}