/**
 * A base class for creating callable objects.
 * See [here](https://stackoverflow.com/q/76073890) for more information.
 *
 * @type {new () => {(...args: any[]): any, _call(...args: any[]): any}}
 */
export const Callable: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
//# sourceMappingURL=generic.d.ts.map