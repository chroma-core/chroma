# Chroma Metering

## Overview

This library provides a procedural-macro based implementation of a metering library that is friendly for multi-threaded, asynchronous, and distributed environments. It allows users to define custom metering **attributes** and **events**. An attribute is a globally unique (in the scope of the crate into which `chroma-metering` is imported) metric, property, quantity, or otherwise that a user wishes to track through metering. An event is a data structure that contains fields onto which attributes can be mapped. An event must have at least one field and events are expected to derive the `std::fmt::Debug`, `std::default::Default`, and `Clone` traits, but beyond this requirement, they are very flexible. Fields within an event may be annotated or unannotated. Unannotated fields are treated as once-settable, meaning that when you call the `create::<YourEvent>()` function, you will need to supply values for these fields. After creation, the library itself provides no methods for modifying these fields, hence "once-settable." It is important to note that they are not _constant_, since when you get the event structure back after calling `close::<YourEvent>()`, you can manually modify the values for these fields. Annotated fields must be annotated with `#[field(attribute = "<a valid attribute>", mutator = "<your mutator fn>")]`. Annotated fields hold the values of the attributes that are used in your event. A mutator is a function that modifies the value of an annotated fields.

## Intended Usage

This library is intended to be used by defining a `metering` module (it can be a file or a folder) somewhere in your project. The single export of this library, `initialize_metering`, is a functional procedural macro that is intended to operate on correctly-annotated attribute and event definitions. No other code (with the exception of comments) should be present in the code on which the macro is invoked.

The macro works by writing the library's source code into your `metering` module, therefore making the metering functions accessible by importing _your own_ metering module. In other words, the only place you will import `chroma_metering` is into **your** `metering` module, and all metering-related functions will be accessible through that module.

## Validations

Internally, the library validates syntax just like the Rust compiler's lexer does so for Rust source code[^1]. In addition to syntactic validations, we perform two semantic validations:

1. No two attributes may have the same name. If this requirement is not met, the library will throw a compile error.
2. Annotated fields must have the same type assignment as the attribute to which they are mapped. Unlike the first requirement, which will throw an error if not satisfied, this requirement is forced by the library to be true. No error is thrown if the user doesn't meet this requirement. This is smoother from a user experience point-of-view, but it's also significantly more opaque because it effectively means that the generated code from the macro contradicts the code the user sees in their editor. In general, macros should have predictable behavior, and this is an example of unpredictable behavior. The reason that we choose to accept this opacity is because there is no way to throw an error for the user at site of the error, since macros are run _during compilation_ and type aliases are not reconciled until _after compilation_. The alternative is to let the compiler pick up the error, but since we don't know the user's type structure, the behavior of the compiler in this scenario varies on a case-by-case basis and is therefore considered undefined.

In addition to the semantic validations performed by the library, there are a number of semantic validations which we allow to be performed implicitly by the compiler:

1. Type aliases attributes must be globally unique within the user's crate.
2. Event struct names must be globally unique within the user's crate.
3. The arguments passed to a mutator must be: a mutable reference to the event in which they are used, followed by a value of the same type as the attribute they are intended to modify. Order matters.
4. Mutators must not have return values.
5. Mutators must be valid symbols within the scope in which the macro is invoked.

## Limitations

- The `#[attribute(...)]`, `#[event]`, and `#[field(...)]` annotations must come before any other macro invocations.
- In the `#[field(...)]` annotation, the `attribute` argument must be supplied before the `mutator` argument.

[^1]: This may not hold for all cases, since we haven't tested our library's syntactic validation as rigorously as the Rust lexer has been tested, but in general, just write syntactically valid Rust code.
