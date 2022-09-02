import React from "react";
import { Datapoint, Projection } from "./types";

// we use this to figure out where to display the on-hover datapoint preview
export const useMousePosition = () => {
    const [
        mousePosition,
        setMousePosition
    ] = React.useState({ x: null, y: null });
    React.useEffect(() => {
        const updateMousePosition = (ev: any) => {
            setMousePosition({ x: ev.clientX, y: ev.clientY });
        };
        window.addEventListener('mousemove', updateMousePosition);
        return () => {
            window.removeEventListener('mousemove', updateMousePosition);
        };
    }, []);
    return mousePosition;
};

// used to find the extents of the plotted points for automatic centering, setting zoom, and setting min/max zoom
export const getBounds = (datapoints: { [key: number]: Datapoint }, projections: { [key: number]: Projection }) => {
    var minX = Infinity
    var minY = Infinity
    var maxX = -Infinity
    var maxY = -Infinity

    Object.values(datapoints).map(function (datapoint) {
        if (projections[datapoint.projection_id!].y < minY) minY = projections[datapoint.projection_id!].y
        if (projections[datapoint.projection_id!].y > maxY) maxY = projections[datapoint.projection_id!].y
        if (projections[datapoint.projection_id!].x < minX) minX = projections[datapoint.projection_id!].x
        if (projections[datapoint.projection_id!].x > maxX) maxX = projections[datapoint.projection_id!].x
    })

    var centerX = (maxX + minX) / 2
    var centerY = (maxY + minY) / 2

    var sizeX = (maxX - minX) / 2
    var sizeY = (maxY - minY) / 2

    return {
        minX: minX,
        maxX: maxX,
        minY: minY,
        maxY: maxY,
        centerX: centerX,
        centerY: centerY,
        maxSize: (sizeX > sizeY) ? sizeX : sizeY
    }
}


export interface ConfigProps {
    scatterplot?: any
}

// used with continuous filters and color interpolation
export function minMaxNormalization(value: number, min: number, max: number) {
    return (value - min) / (max - min)
}

// this has to be out side of react because we need to keep its reference constant
export function selectCallbackOutsideReact(points: any) {
    // @ts-ignore
    window.selectHandler(points)
}

// this has to be out side of react because we need to keep its reference constant
export function viewCallbackOutsideReact(viewData: any) {
    // @ts-ignore
    window.viewHandler(viewData)
}

export interface PlotterProps {
    allFetched: boolean
}