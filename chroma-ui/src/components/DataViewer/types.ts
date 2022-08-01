import { Atom } from "jotai"

export interface Annotation {
  id: number
  category_id: number
  area: number
  bbox: number[]
  iscrowd: number
  image_id: number
  segmentation: number[]
}

export interface Datapoint {
  dataset_id: number
  id: number
  inference_id: number
  label_id: number
  metadata: {}
  resource_id: number
  tag_ids: number[]
  annotations: Annotation[]
  inferences: Annotation[]
  projection_id: number
  visible: boolean
}

export interface Dataset {
  id: number
  name: string
  datapoints: number[]
  categories: string
}

export interface Projection {
  id: number
  x: number
  y: number
  datapoint: number
}

export interface Inference {
  id: number
  data: string
  datapoint: number
}

export interface Label {
  id: number
  data: string
  datapoint: number
}

export interface Resource {
  id: number
  uri: string
  datapoint: number
}

export interface Tag {
  id: number
  name: string
  datapoint_ids: number[]
}

export interface Category {
  id: number
  name: string
  datapoints: number[]
}

export interface NormalizeData {
  entities: {
    datapoints: { [key: number]: Datapoint }
    datasets: { [key: number]: Dataset }
    inferences: { [key: number]: Inference }
    labels: { [key: number]: Label }
    resources: { [key: number]: Resource }
    tags: { [key: number]: Tag }
    categories: { [key: number]: Category }
    projections: { [key: number]: Projection }
  }
}

export const CursorMap: { [key: string]: string } = {
  select: "select-cursor",
  lasso: "crosshair",
  add: "crosshair-plus-cursor",
  remove: "crosshair-minus-cursor",
}

export enum FilterType {
  Discrete,
  Continuous,
}

export interface FilterOption {
  visible: boolean
  color: string
  id: number
  evalDatapoint: (datapoint: Datapoint, option: FilterOption) => boolean
}

interface FilterRange {
  min: number
  max: number
  minVisible: number
  maxVisible: number
  colorScale: string[]
}

export interface Filter {
  name: string
  type: FilterType,
  options?: FilterOption[]
  range?: FilterRange
  linkedAtom?: any
}

// dataset, category (inference, and labeled), tag, <-- these are actual collections that have ids associated...... 
// quality <-- this is in metadata on the object
// agree/disagree <-- this is, right now, generated at run-time


export interface FilterArray {
  filter: Filter
  update: (updateObject: any) => void
}