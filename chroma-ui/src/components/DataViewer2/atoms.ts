import { atom } from "jotai"
import { Datapoint, Dataset, Label, Tag, Resource, Inference, Category, Projection, CursorMap, Filter } from "./types"

// Core data atoms
export const datapointsAtom = atom<{[key: number]: Datapoint}>({})
export const datasetsAtom = atom<{[key: number]: Dataset}>({})
export const labelsAtom = atom<{[key: number]: Label}>({})
export const tagsAtom = atom<{[key: number]: Tag}>({})
export const resourcesAtom = atom<{[key: number]: Resource}>({})
export const inferencesAtom = atom<{[key: number]: Inference}>({})
export const categoriesAtom = atom<{[key: number]: Category}>({})
export const projectionsAtom = atom<{[key: number]: Projection}>({})

// Visbility state
export const selectedDatapointsAtom = atom<number[]>([]) // positive selection list
export const visibleDatapointsAtom = atom<number[]>([]) // negative selection list
// can i create an atom which is visible... which takes is all the datapoints, remove hidden, selects (if applicable) selected

// UI state atoms
export const toolSelectedAtom = atom<string>('cursor')
export const toolWhenShiftPressedAtom = atom<string>('')
export const cursorAtom = atom<string>(CursorMap.select)
export const colsPerRowAtom = atom<number>(3)
export const datapointModalIndexAtom = atom<number>(0)
export const datapointModalOpenAtom = atom<boolean>(false)
export const pointsToSelectAtom = atom<number[]>([])

// Filter state
export const inferenceFilterAtom = atom<Filter | undefined>(undefined)
export const categoryFilterAtom = atom<Filter | undefined>(undefined)
export const tagFilterAtom = atom<Filter | undefined>(undefined)
export const qualityFilterAtom = atom<Filter | undefined>(undefined)
export const datasetFilterAtom = atom<Filter | undefined>(undefined)

