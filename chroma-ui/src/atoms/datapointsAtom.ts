import { atom } from "jotai";
import { atomWithQuery } from "jotai/query";

// const datapointsAtom = atomWithQuery(() => ({
//   queryKey: "datapoints",
//   queryFn: async (): Promise<any[]> => {
//     const res = await fetch("/api/datapoints/1");
//     return res.json();
//   }
// }));

const datapointsAtom = atom(undefined)

export { datapointsAtom }