
/**
 * A base class for creating callable objects.
 * See [here](https://stackoverflow.com/q/76073890) for more information.
 * 
 * @type {new () => {(...args: any[]): any, _call(...args: any[]): any}}
 */
export const Callable = /** @type {any} */ (class {
    /**
    * Creates a new instance of the Callable class.
    */
    constructor() {
        /**
         * Creates a closure that delegates to a private method '_call' with the given arguments.
         * @type {any}
         * @param {...any} args Zero or more arguments to pass to the '_call' method.
         * @returns {*} The result of calling the '_call' method.
         */
        let closure = function (...args) {
            return closure._call(...args)
        }
        return Object.setPrototypeOf(closure, new.target.prototype)
    }

    /**
     * This method should be implemented in subclasses to provide the
     * functionality of the callable object.
     *
     * @param {any[]} args
     * @throws {Error} If the subclass does not implement the `_call` method.
     */
    _call(...args) {
        throw Error('Must implement _call method in subclass')
    }
});
