/**
 * Function that mimics Python's range() function.
 * @param start The start value of the range.
 * @param stop The stop value of the range. If not provided, start will be 0 and stop will be the provided start value.
 * @param step The step value of the range. Defaults to 1.
 * @returns The range of numbers.
 */
export function range(start: number, stop?: number, step = 1): number[] {
	if (stop === undefined) {
		stop = start;
		start = 0;
	}

	const result: number[] = [];
	for (let i = start; i < stop; i += step) {
		result.push(i);
	}
	return result;
}

/**
 * Function that mimics Python's array slicing.
 * @param array The array to slice.
 * @param start The start index of the slice. Defaults to 0.
 * @param stop The last index of the slice. Defaults to `array.length`.
 * @param step The step value of the slice. Defaults to 1.
 * @returns The sliced array.
 */
export function slice<T>(array: T[], start?: number, stop?: number, step = 1): T[] {
	const direction = Math.sign(step);

	if (direction >= 0) {
		start = (start ??= 0) < 0 ? Math.max(array.length + start, 0) : Math.min(start, array.length);
		stop = (stop ??= array.length) < 0 ? Math.max(array.length + stop, 0) : Math.min(stop, array.length);
	} else {
		start = (start ??= array.length - 1) < 0 ? Math.max(array.length + start, -1) : Math.min(start, array.length - 1);
		stop = (stop ??= -1) < -1 ? Math.max(array.length + stop, -1) : Math.min(stop, array.length - 1);
	}

	const result: T[] = [];
	for (let i = start; direction * i < direction * stop; i += step) {
		result.push(array[i]);
	}
	return result;
}

/**
 * Function that mimics Python's string.title() function.
 * @param value The string to title case.
 * @returns The title cased string.
 */
export function titleCase(value: string): string {
	return value.replace(/\b\w/g, (c) => c.toUpperCase());
}
