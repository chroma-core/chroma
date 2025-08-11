# Chroma Metering

## Overview

This library provides a procedural-macro based implementation of a metering library that is friendly for multi-threaded, asynchronous, and distributed environments. It allows users to define custom metering **capabilities** and **contexts**. An capability is globally unique (in the scope of the crate into which `chroma-metering` is imported) and represents the property of a context that allows it to react via a **handler**. An context is a data structure that contains fields. Fields may be mutated by handlers (not necessarily 1:1) when a capability is invoked. A context must have at least one field and contexts are expected to be `Debug`, `Any`, `Send`, and `Sync`.

## Intended Usage

This library is intended to be used by defining a `metering` module (it can be a file or a folder) somewhere in your project. The single export of this library, `initialize_metering`, is a functional procedural macro that is intended to operate on correctly-annotated capability and context definitions. No other code (with the exception of comments) should be present in the code on which the macro is invoked.

The macro works by writing the library's source code into your `metering` module, therefore making the metering functions accessible by importing _your own_ metering module. In other words, the only place you will import `chroma_metering` is into **your** `metering` module, and all metering-related functions will be accessible through that module.
