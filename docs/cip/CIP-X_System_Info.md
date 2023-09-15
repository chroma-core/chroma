# CIP-X System Info

## Status

Current Status: `Under Discussion`

## Motivation

Currently, a lot of the support discussions in Discord revolve around gathering information about the user's operating
environment. This information is crucial for debugging and troubleshooting issues. We want to make it easier for users
to provide this information.

## Public Interfaces

This proposal introduces a new `API` method `system_info` that will return a dictionary with system information based on
flags provided by the user.

We also suggest the introduction of two cli commands:

- `chroma system-info` that will print the system information to the console.
- `chroma rstat` that will continuously print CPU and memory usage statistics to the console.

## Proposed Changes

The proposed changes are mentioned in the public interfaces.

## Compatibility, Deprecation, and Migration Plan

This change is backward compatible.

## Test Plan

We plan to modify unit tests to accommodate the change and use system tests to verify
this API change is backward compatible.

## Rejected Alternatives

TBD
