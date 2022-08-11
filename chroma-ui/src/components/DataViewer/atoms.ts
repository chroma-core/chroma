import { atom } from "jotai"
import { Datapoint, Dataset, Label, Tag, Resource, Inference, Category, Projection, CursorMap, Filter } from "./types"

// Core data atoms
export const datapointsAtom = atom<{ [key: number]: Datapoint }>({})
export const datasetsAtom = atom<{ [key: number]: Dataset }>({})
export const labelsAtom = atom<{ [key: number]: Label }>({})
export const tagsAtom = atom<{ [key: number]: Tag }>({})
export const resourcesAtom = atom<{ [key: number]: Resource }>({})
export const inferencesAtom = atom<{ [key: number]: Inference }>({})
export const categoriesAtom = atom<{ [key: number]: Category }>({})
export const projectionsAtom = atom<{ [key: number]: Projection }>({})
export const metadataFiltersAtom = atom<{ [key: number]: any }>({})

export const labelDatapointsAtom = atom<{ [key: number]: Datapoint }>({})
export const labelDatasetsAtom = atom<{ [key: number]: Dataset }>({})
export const labelLabelsAtom = atom<{ [key: number]: Label }>({})
export const labelTagsAtom = atom<{ [key: number]: Tag }>({})
export const labelResourcesAtom = atom<{ [key: number]: Resource }>({})
export const labelInferencesAtom = atom<{ [key: number]: Inference }>({})
export const labelCategoriesAtom = atom<{ [key: number]: Category }>({})
export const labelProjectionsAtom = atom<{ [key: number]: Projection }>({})
export const labelMetadataFiltersAtom = atom<{ [key: number]: any }>({})

export const globalProjectionsAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(projectionsAtom)
        return get(labelProjectionsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = projectionsAtom
        if (contextObject == DataType.Object) localAtom = labelProjectionsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalDatapointAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(datapointsAtom)
        return get(labelDatapointsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = datapointsAtom
        if (contextObject == DataType.Object) localAtom = labelDatapointsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalResourcesAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(resourcesAtom)
        return get(labelResourcesAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = resourcesAtom
        if (contextObject == DataType.Object) localAtom = labelResourcesAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalVisibleDatapointsAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(visibleDatapointsAtom)
        return get(labelVisibleDatapointsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = visibleDatapointsAtom
        if (contextObject == DataType.Object) localAtom = labelVisibleDatapointsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalSelectedDatapointsAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(selectedDatapointsAtom)
        return get(labelSelectedDatapointsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = selectedDatapointsAtom
        if (contextObject == DataType.Object) localAtom = labelSelectedDatapointsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

// Visbility state
export const selectedDatapointsAtom = atom<number[]>([]) // positive selection list
export const visibleDatapointsAtom = atom<number[]>([]) // negative selection list
export const labelSelectedDatapointsAtom = atom<number[]>([]) // positive selection list
export const labelVisibleDatapointsAtom = atom<number[]>([]) // negative selection list
// can i create an atom which is visible... which takes is all the datapoints, remove hidden, selects (if applicable) selected

// UI state atoms
export const toolSelectedAtom = atom<string>('cursor')
export const toolWhenShiftPressedAtom = atom<string>('')
export const cursorAtom = atom<string>(CursorMap.select)
export const colsPerRowAtom = atom<number>(3)
export const datapointModalIndexAtom = atom<number>(0)
export const datapointModalOpenAtom = atom<boolean>(false)
export const pointsToSelectAtom = atom<number[]>([])

export enum DataType {
    Context,
    Object,
}
export const contextObjectSwitcherAtom = atom<number>(DataType.Context)

// Filter state
export const inferenceFilterAtom = atom<Filter | undefined>(undefined)
export const categoryFilterAtom = atom<Filter | undefined>(undefined)
export const tagFilterAtom = atom<Filter | undefined>(undefined)
export const qualityFilterAtom = atom<Filter | undefined>(undefined)
export const datasetFilterAtom = atom<Filter | undefined>(undefined)

export const labelInferenceFilterAtom = atom<Filter | undefined>(undefined)
export const labelCategoryFilterAtom = atom<Filter | undefined>(undefined)
export const labelTagFilterAtom = atom<Filter | undefined>(undefined)
export const labelQualityFilterAtom = atom<Filter | undefined>(undefined)
export const labelDatasetFilterAtom = atom<Filter | undefined>(undefined)

export const globalCategoryFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(categoryFilterAtom)
        return get(labelCategoryFilterAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = categoryFilterAtom
        if (contextObject == DataType.Object) localAtom = labelCategoryFilterAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalDatasetFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(datasetFilterAtom)
        return get(labelDatasetFilterAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = datasetFilterAtom
        if (contextObject == DataType.Object) localAtom = labelDatasetFilterAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalMetadataFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(metadataFiltersAtom)
        return get(labelMetadataFiltersAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = metadataFiltersAtom
        if (contextObject == DataType.Object) localAtom = labelMetadataFiltersAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalTagFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(tagFilterAtom)
        return undefined
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = tagsAtom
        if (contextObject == DataType.Object) localAtom = undefined
        // @ts-ignore
        set(localAtom, dps!)
    })