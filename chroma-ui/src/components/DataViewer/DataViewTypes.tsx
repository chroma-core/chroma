export interface ProjectionSet {
    id: string
    createdAt: string
    updatedAt: string
}

enum FilterType {
    Discrete,
    Continuous,
}

interface Filter {
    type: FilterType
    filterByFunction: () => void
    sortByFunction: () => void
    colorByFunciton: () => void
    active: boolean
}

interface TagItem {
    left_id?: number
    right_id?: number
    tag: {
        id?: number
        name: string
    }
}

export interface Datapoint {
    id: number
    dataset: {
        id: number
        name: string
    }
    label: {
        id: number
        data: any
    }
    resource: {
        id: number
        uri: string
    }
    tags: TagItem[]
    visible?: boolean
    projection?: {
        id: number
        x: number
        y: number
    }
}

interface Projection {
    id: number
    x: number
    y: number
    embedding: {
        id: number
        datapoint: {
            id: number
        }
    }
}

interface ProjectionSetData {
    id: number
    projections: Projection[]
}

interface ViewerData {
    projectId: number // onload set 
    filters: Filter[] // need to generate this... 
    datapoints: Datapoint[] // onload should have this filled out
    projectionSets: ProjectionSetData[] // onload should have one of these
}

export interface ProjectionData {
    projections?: Projection[] // onload should have this filled out
}

interface FilterSidebar {

}

interface PointPlotter {
    colorByFilter: string // the key/name of the filter?
}

interface DataSidebar {
    sortByFilter: string // the key/name of the filter?
}
