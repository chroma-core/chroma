# Installation
> `npm install --save @types/normalize-package-data`

# Summary
This package contains type definitions for normalize-package-data (https://github.com/npm/normalize-package-data#readme).

# Details
Files were exported from https://github.com/DefinitelyTyped/DefinitelyTyped/tree/master/types/normalize-package-data.
## [index.d.ts](https://github.com/DefinitelyTyped/DefinitelyTyped/tree/master/types/normalize-package-data/index.d.ts)
````ts
export = normalize;

declare function normalize(data: normalize.Input, warn?: normalize.WarnFn, strict?: boolean): void;
declare function normalize(data: normalize.Input, strict?: boolean): void;

declare namespace normalize {
    type WarnFn = (msg: string) => void;
    interface Input {
        [k: string]: any;
    }

    interface Person {
        name?: string | undefined;
        email?: string | undefined;
        url?: string | undefined;
    }

    interface Package {
        [k: string]: any;
        name: string;
        version: string;
        files?: string[] | undefined;
        bin?: { [k: string]: string } | undefined;
        man?: string[] | undefined;
        keywords?: string[] | undefined;
        author?: Person | undefined;
        maintainers?: Person[] | undefined;
        contributors?: Person[] | undefined;
        bundleDependencies?: { [name: string]: string } | undefined;
        dependencies?: { [name: string]: string } | undefined;
        devDependencies?: { [name: string]: string } | undefined;
        optionalDependencies?: { [name: string]: string } | undefined;
        description?: string | undefined;
        engines?: { [type: string]: string } | undefined;
        license?: string | undefined;
        repository?: { type: string; url: string } | undefined;
        bugs?: { url: string; email?: string | undefined } | { url?: string | undefined; email: string } | undefined;
        homepage?: string | undefined;
        scripts?: { [k: string]: string } | undefined;
        readme: string;
        _id: string;
    }
}

````

### Additional Details
 * Last updated: Tue, 07 Nov 2023 09:09:39 GMT
 * Dependencies: none

# Credits
These definitions were written by [Jeff Dickey](https://github.com/jdxcode).
