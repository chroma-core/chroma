# CIP-01302024: Schedulers

## Status

Current Status: `Under Discussion`

## **Motivation**

Chroma does not have a way to carry out pro-active maintenance tasks. The alternatives are as follows:

- Expose API to allow maintenance tasks to be carried out by the user
- Manual maintenance tasks, on a stopped database
- Standard API triggered maintenance tasks (reactive maintenance)
- Scheduler-based maintenance tasks (proactive maintenance)

In this CIP, we propose the introduction of a scheduler-based maintenance task system by discussing the pros and
cons of the available alternatives.

## **Public Interfaces**

No changes to public interfaces are proposed.

## **Proposed Changes**

### Exploration of Alternatives

#### Expose API to allow maintenance tasks to be carried out by the user

This is seemingly the simplest approach. However, it comes with a number of downsides. The main one being that it
requires the user to be aware of the maintenance tasks that need to be carried out and the order in which they need
to be carried out.

Here is a non-exhaustive summary of potential downsides:

- Increase API surface area (on top of larger API this introduces additional attack vectors, especially with maintenance
  tasks)
- Increase the number of "features" that need to be maintained across, versions and deployment types.
- Give too many knobs to turn to users makes for a bad user experience. We also may not want to expose certain
  features to users which fragments the set of maintenance tasks.
- The potential for users to break their database by running maintenance tasks in the wrong order or at the wrong time.

#### Manual maintenance tasks, on a stopped database

Manual tasks are relatively simpler to implement but come at the expense of user experience and the need to have your
database down for the duration of the maintenance task.

#### Standard API triggered maintenance tasks (reactive maintenance)

Standard API triggered maintenance tasks refers to the concept of having a maintenance task triggered by a specific
API call. For example, a index off-loading from memory triggered upon another index being loaded (e.g. LRU style cache).

This approach is relatively simple to implement and does not require the user to be aware of the maintenance tasks
that need to be carried out. However, it comes with a number of downsides:

- It relies on API call to trigger a maintenance task. This puts direct dependency on the API usage to be able to
  trigger
  the maintenance task.
- It may potentially have side effects that can cause the main API call to fail (can be avoided with some
  consideration).
- It may introduce performance issues if the maintenance task is triggered synchronously (can be avoided with some
  consideration).

#### Scheduler-based maintenance tasks (proactive maintenance)

Scheduler-based maintenance tasks refers to the concept of having a maintenance task triggered by a scheduler and only
executed upon meeting certain criteria. For example, a index off-loading from memory triggered upon another index being
loaded (e.g. LRU style cache).

This approach is similar to the reactive maintenance approach, but does not require API usage to be triggered.

Some of the drawbacks of this approach are:

- It may have performance impact if triggered maintenance task coincide with peek API usage.
- It may invalidate certain user expectations (e.g. if the user expects the index to be loaded whereas it is not).
- Conflicting access to the same resource (e.g. index) may cause failures in either API call or maintenance task - can
  be mitigated with considerations.

#### Conclusion

In our research we found that proactive maintenance tasks via scheduler-based triggers, even with its drawbacks, is
the best fit for Chroma. The scheduler-based approach strikes a good balance between user experience, flexibility in
configuration by exposing a number of knobs to the user, and the ability to carry out maintenance tasks in a timely
manner.

Additional considerations (non-exhaustive):

- Resource Management and prioritization - the scheduler should be flexible enough to allow for user-triggered API calls
  to take precedence over maintenance tasks. The scheduler should be capable of handling missed maintenance tasks
- Customization and configuration - the scheduler should have sufficient abstraction to allow for customizing its
  behaviour or implementing different schedulers. Sufficient configuration should be exposed to the user to allow for a
  good user experience.
- ... (to be expanded)

### The Abstractions

To allow for future extensibility, we propose the introduction of `ChromaScheduler` abstraction with the following
definition:

```python
from abc import abstractmethod
from datetime import datetime
from typing import Callable, Optional

from chromadb.config import Component, System


class ChromaScheduler(Component):

  def __init__(self, system: System):
    super().__init__(system)

  @abstractmethod
  def schedule_interval(self, *, job: Callable[..., ...], interval: int) -> None:
    """
    Schedule a job to run on an interval. The interval is in seconds.
    """
    ...

  @abstractmethod
  def schedule_cron(self, *, job: Callable[..., ...], cron_expr: str) -> None:
    """
    Schedule a job to run on a cron expression.
    """
    ...

  @abstractmethod
  def schedule_once(self, *, job: Callable[..., ...], delay: Optional[int], date_time: Optional[datetime]) -> None:
    """
    Schedule a job to run once after a delay or at a specific date/time.
    If both delay and date_time are None, raise an error.
    """
    ...
```

#### Alternative Implementations

In our research we have considered the following alternatives:

- stdlib's `sched` module
- `schedule` library
- `APScheduler` library
- `Celery` library
- `python-crontab` library

#### Architectural Considerations

Given the use cases of Chroma we have prioritized the following architectural considerations:

- Ease of use
- Library maturity
- Ability to run in-memory and persist jobs
- Ability to run in in-process as well as good support for distributed environment
- Minimal footprint (including dependencies)

#### Implementation Comparison

| Library          | Ease of use | Maturity | Persistence | Distributed | Footprint |
|------------------|-------------|----------|-------------|-------------|-----------|
| stdlib's `sched` | ✅           | ✅        | ❌           | ❌           | ✅         |
| `schedule`       | ✅           | ✅        | ❌           | ❌           | ✅         |
| `APScheduler`    | ✅           | ✅        | ✅           | ✅           | ✅         |
| `Celery`         | ❌           | ✅        | ✅           | ✅           | ❌         |
| `python-crontab` | ❌           | ✅        | ❌           | ❌           | ✅         |

Conclusions:

- `sched` while being part of the stdlib, is too simplistic and will require re-inventing the wheel on many high-level
  features. It also lacks the ability to persist jobs state and will require substantial effort to make it work in
  distributed environments.
- `schedule` is a good library, however it lacks the ability to persist jobs state and will require some effort to make
  it work in distributed environments.
- `Celery` is a good library for distributed scheduling of tasks. However, it comes with some hefty requirements which
  are overwhelming at this stage of Chroma's development, even more so for single-node Chroma.
- `python-chromtab` is a good library for scheduling cron jobs. However, it lacks the ability to persist jobs state and
  will require some effort to make it work in distributed environments.
- `APScheduler` is a good library, with a lot of features and active community. It is relatively well-documented. It
  does not provide explicit distributed support, however it is possible to run it in distributed environments with some
  consideration.

> Note: Nothing in the above comparison precludes the use of the above libraries in Chroma. However, we believe
> that `APScheduler` is the best fit for Chroma as a default implementation.

### Default Implementation

We do not wish to impose a specific scheduler implementation on the user. However, in this CIP, we propose the use
of `APScheduler` as the default implementation. The reason is that it is a well established library with active
community. Further, we believe that its flexibility of supported use cases and the ability to persist jobs is a good
fit for Chroma.

### Scheduler Configuration

We propose that schedulers are configured via standard Chroma settings approach. Whereby at the very least the user
should be able to configure the scheduler implementation and its configuration. For example:

```python
chroma_scheduler_class = "chromadb.scheduling.apscheduler.APScheduler"
```

### Practical Use Cases of Schedulers

Below are some practical examples we believe.

- Memory and cache management
- WAL-to-HNSW index synchronization (single-node Chroma)
- WAL pruning (single-node Chroma)
- SQLite file management (e.g. VACUUM) (single-node Chroma)
- WAL-based backups
- Index rebuilds

> Note: It is important to observe that the above use cases are not exhaustive and come with their respective impacts
> and trade-offs.

## **Compatibility, Deprecation, and Migration Plan**

The change will be backward compatible with existing release 0.4.x. However, it is important to note that no backporting
of this feature to previous released in the 0.4.x series is proposed.

## **Test Plan**

Property tests will be added to ensure that the scheduler implementation is working as expected.

## **Rejected Alternatives**

N/A
