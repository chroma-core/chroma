## ⚠️ This document is a work in progress

## Limitations

Since this library is undergoing active development, there are some limitations that users must be aware of to ensure that all functionality works as expected:

- **Colliding event names:** Metering events for a given crate must have globally unique struct names, even if they are defined in different scopes. This is because we have not yet implemented our own one-way hash function over the tokens that define each event and their file paths, so we rely on the name of the event as a key in our registry.
- **Weak validation:** The syntactic validation this library offers for the arguments to its macros is not comprehensive and often may result in opaque error messages.
- **IDE pain:** Because of our heavy reliance on generated code, some IDEs are slow to respond to changes in code and may falsely report compilation errors. The best way around this is to check compilation manually using a terminal. You may also restart your language server, but that can be a slow and interruptive process.
- **Registry fragility:** The read and write techniques used on the registry are still in somewhat of a primitive state. Manual changes to the registry will likely break compilation.
