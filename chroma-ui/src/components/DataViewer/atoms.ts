import { atom } from "jotai"
import { Datapoint, Dataset, Label, Tag, Resource, Inference, Category, Projection, CursorMap, Filter, PlotterBounds } from "./types"

// Core data atoms
export const context__datapointsAtom = atom<{ [key: number]: Datapoint }>({})
export const context__datasetsAtom = atom<{ [key: number]: Dataset }>({})
export const context__labelsAtom = atom<{ [key: number]: Label }>({})
export const context__tagsAtom = atom<{ [key: number]: Tag }>({})
export const context__resourcesAtom = atom<{ [key: number]: Resource }>({})
export const context__inferencesAtom = atom<{ [key: number]: Inference }>({})
export const context__categoriesAtom = atom<{ [key: number]: Category }>({})
export const context__projectionsAtom = atom<{ [key: number]: Projection }>({})
export const context__metadataFiltersAtom = atom<{ [key: number]: any }>({})

export const object__datapointsAtom = atom<{ [key: number]: Datapoint }>({})
export const object__datasetsAtom = atom<{ [key: number]: Dataset }>({})
export const object__labelsAtom = atom<{ [key: number]: Label }>({})
export const object__tagsAtom = atom<{ [key: number]: Tag }>({})
export const object__resourcesAtom = atom<{ [key: number]: Resource }>({})
export const object__inferencesAtom = atom<{ [key: number]: Inference }>({})
export const object__categoriesAtom = atom<{ [key: number]: Category }>({})
export const object__projectionsAtom = atom<{ [key: number]: Projection }>({})
export const object__metadataFiltersAtom = atom<{ [key: number]: any }>({})

export const globalProjectionsAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__projectionsAtom)
        return get(object__projectionsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__projectionsAtom
        if (contextObject == DataType.Object) localAtom = object__labelsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalTagsAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__tagsAtom)
        return get(object__tagsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__tagsAtom
        if (contextObject == DataType.Object) localAtom = object__tagsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalCategoriesAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__categoriesAtom)
        return get(object__categoriesAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__categoriesAtom
        if (contextObject == DataType.Object) localAtom = object__categoriesAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalDatapointAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__datapointsAtom)
        return get(object__datapointsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__datapointsAtom
        if (contextObject == DataType.Object) localAtom = object__datapointsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalResourcesAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__resourcesAtom)
        return get(object__resourcesAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__resourcesAtom
        if (contextObject == DataType.Object) localAtom = object__resourcesAtom
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

export const hoverToHighlightInPlotterDatapointIdAtom = atom<number | undefined>(undefined)

export const context__datapointToPointMapAtom = atom<number[]>([])
export const object_datapointToPointMapAtom = atom<number[]>([])

export const globaldatapointToPointMapAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__datapointToPointMapAtom)
        return get(object_datapointToPointMapAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__datapointToPointMapAtom
        if (contextObject == DataType.Object) localAtom = object_datapointToPointMapAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const context__plotterBoundsAtom = atom<PlotterBounds>({})
export const object_plotterBoundsAtom = atom<PlotterBounds>({})

export const globalplotterBoundsAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__plotterBoundsAtom)
        return get(object_plotterBoundsAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__plotterBoundsAtom
        if (contextObject == DataType.Object) localAtom = object_plotterBoundsAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export enum DataType {
    Context,
    Object,
}
export const contextObjectSwitcherAtom = atom<number>(DataType.Context)

// Filter state
export const context__inferenceFilterAtom = atom<Filter | undefined>(undefined)
export const context__categoryFilterAtom = atom<Filter | undefined>(undefined)
export const context__tagFilterAtom = atom<Filter | undefined>(undefined)
export const context__qualityFilterAtom = atom<Filter | undefined>(undefined)
export const context__datasetFilterAtom = atom<Filter | undefined>(undefined)

export const object__inferenceFilterAtom = atom<Filter | undefined>(undefined)
export const object__categoryFilterAtom = atom<Filter | undefined>(undefined)
export const object__tagFilterAtom = atom<Filter | undefined>(undefined)
export const object__qualityFilterAtom = atom<Filter | undefined>(undefined)
export const object__datasetFilterAtom = atom<Filter | undefined>(undefined)

export const globalCategoryFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__categoryFilterAtom)
        return get(object__categoryFilterAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__categoryFilterAtom
        if (contextObject == DataType.Object) localAtom = object__categoryFilterAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalDatasetFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__datasetFilterAtom)
        return get(object__datasetFilterAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__datasetFilterAtom
        if (contextObject == DataType.Object) localAtom = object__datasetFilterAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalMetadataFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__metadataFiltersAtom)
        return get(object__metadataFiltersAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__metadataFiltersAtom
        if (contextObject == DataType.Object) localAtom = object__metadataFiltersAtom
        // @ts-ignore
        set(localAtom, dps!)
    })

export const globalTagFilterAtom = atom(
    (get) => {
        const contextObject = get(contextObjectSwitcherAtom)
        if (contextObject == DataType.Context) return get(context__tagFilterAtom)
        return get(object__tagFilterAtom)
    },
    (get, set, dps?: any) => {
        const contextObject = get(contextObjectSwitcherAtom)
        let localAtom
        if (contextObject == DataType.Context) localAtom = context__tagFilterAtom
        if (contextObject == DataType.Object) localAtom = object__tagFilterAtom
        // @ts-ignore
        set(localAtom, dps!)
    })