// @ts-nocheck
import { useQuery, useSubscription } from 'urql';

const handleSubscription = (response, message) => {
  return message;
};

export default function TestUqrl() {
  const [res] = useSubscription({ query: newMessages }, handleSubscription);

  const { data, fetching, error } = result;

  if (fetching) return <p>Loading...</p>;
  if (error) return <p>Oh no... {error.message}</p>;

  return (
    <>
      <h1>subscription - off</h1>
      <ul>
        {res.data.count}
      </ul>
    </>
  );
}