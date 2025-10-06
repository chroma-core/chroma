# CIP Chroma Improvement Proposals

## Purpose

We want to make Chroma a core architectural component for users. Core architectural
elements can't break compatibility or shift functionality from release to release.
As a result each new major feature or public api has to be done in a way that we can stick
with it going forward.

This means when making this kind of change we need to think through what we are doing as
best we can prior to release. And as we go forward we need to stick to our decisions as
much as possible. All technical decisions have pros and cons so it is important we
capture the thought process that leads to a decision or design to avoid flip-flopping
needlessly.

Hopefully we can make these proportional in effort to their magnitude — small changes
should just need a couple brief paragraphs, whereas large changes need detailed design
discussions.

This process also isn't meant to discourage incompatible changes — proposing an
incompatible change is totally legitimate. Sometimes we will have made a mistake and
the best path forward is a clean break that cleans things up and gives us a good
foundation going forward. Rather this is intended to avoid accidentally introducing
half thought-out interfaces and protocols that cause needless heartburn when changed.
Likewise the definition of "compatible" is itself squishy: small details like which
errors are thrown when are clearly part of the contract but may need to change in some
circumstances, likewise performance isn't part of the public contract but dramatic
changes may break use cases. So we just need to use good judgement about how big the
impact of an incompatibility will be and how big the payoff is.

## What is considered a "major change" that needs a CIP?

- Any of the following should be considered a major change:
  - Any major new feature, subsystem, or piece of functionality
  - Any change that impacts the public interfaces of the project

What are the "public interfaces" of the project?

All of the following are public interfaces that people build around:

- Index or Metadata storage format
- The network protocol
- The api behavior
- Configuration, especially client configuration
- Monitoring
- Command line tools and arguments

## What should be included in a CIP?

A CIP should contain the following sections:

- Motivation: describe the problem to be solved
- Impact: describe what percentage of users do we think will be impacted by the proposed change.
- Proposed Change: describe the new thing you want to do. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences, depending on the scope of the change.
- New or Changed Public Interfaces: impact to any of the "compatibility commitments" described above. We want to call these out in particular so everyone thinks about them.
- Migration Plan and Compatibility: if this feature requires additional support for a no-downtime upgrade describe how that will work
- Rejected Alternatives: What are the other alternatives you considered and why are they worse? The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

## Who should initiate the CIP?

Anyone can initiate a CIP - we welcome ideas about how to improve Chroma, the core
Chroma team will review, provide feedback, and come to a decision on if the proposal
makes sense for the long term direction of Chroma.
