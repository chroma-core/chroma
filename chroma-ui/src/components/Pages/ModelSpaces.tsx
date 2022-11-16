import { useEffect, useState } from "react";

export default function Projects() {
  const [modelSpaces, setModelSpaces] = useState([]);
   useEffect(() => {
      fetch('http://localhost:8000/api/v1/model_spaces')
         .then((response) => response.json())
         .then((data) => {
            console.log(data);
            setModelSpaces(data);
         })
         .catch((err) => {
            console.log(err.message);
         });
   }, []);

  return (
    <>
      {modelSpaces?.map(modelSpace => (
         <p key={modelSpace}>{modelSpace}</p>
        ))}
    </>
  )
}