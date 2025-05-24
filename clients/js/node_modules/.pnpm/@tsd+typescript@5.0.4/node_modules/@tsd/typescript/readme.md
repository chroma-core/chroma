# @tsd/typescript ![CI](https://github.com/SamVerschueren/tsd-typescript/workflows/CI/badge.svg)

> TypeScript with some extras for type-checking.

This is a drop-in replacement for [TypeScript](https://github.com/microsoft/TypeScript) meant for programmatic usage only. It does not expose the binaries like `tsc` and `tsserver` because it would override the TypeScript binaries of the project. It exposes extra methods on the internal `TypeChecker` object.


## Install

```
npm install --save-dev @tsd/typescript
```


## Usage

This package is just TypeScript with some private methods exposed that [tsd](https://github.com/SamVerschueren/tsd) needs for enhanced type checking.

- `isTypeIdenticalTo(a: Type, b: Type)`: Check if two types are identical to each other. [More info...](https://github.com/microsoft/TypeScript/blob/v4.2.4/doc/spec-ARCHIVED.md#3.11.2)
- `isTypeSubtypeOf(a: Type, b: Type)`: Check if type `a` is a subtype of type `b`. [More info...](https://github.com/microsoft/TypeScript/blob/v4.2.4/doc/spec-ARCHIVED.md#3.11.3)
- `isTypeAssignableTo(a: Type, b: Type)`: Check if type `a` is assignable to type `b`.
- `isTypeDerivedFrom(a: Type, b: Type)`: Check if type `a` is derived from type `b`. [More info...](https://github.com/SamVerschueren/tsd-typescript/blob/master/scripts/utils/replacements.js#L65-L74)
- `isTypeComparableTo(a: Type, b: Type)`: Check if type `a` is comparable to type `b`. [More info...](https://github.com/SamVerschueren/tsd-typescript/blob/master/scripts/utils/replacements.js#L77-L86)
- `areTypesComparable(a: Type, b: Type)`: Check if type `a` is comparable to type `b` and `b` is comparable to type `a`. [More info...](https://github.com/SamVerschueren/tsd-typescript/blob/master/scripts/utils/replacements.js#L89-L98)


## Related

- [tsd](https://github.com/SamVerschueren/tsd) - Check TypeScript type definitions
