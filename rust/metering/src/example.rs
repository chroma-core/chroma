use crate::{declare_event_capabilities, impl_dyn_event_forwarders, impl_event_capabilities};
use std::any::Any;

/**

We assume the following design guidelines

## Callsite Ergonomics
We care a lot about the callsite ergonomics of creating, submitting and using capabilities!
1. Callers that want to use a capability given a current metering context
    should not have to know the concrete type of the metering context.
2. Such callers should not have to cast the metering context in order to use the capability.
3. As such all capabilities must be accessible on the base MeteringContext.
4. We want capabilities to be strongly typed, as opposed to using AnyMaps, string, or
    other weakly typed mechanisms.
5. To the caller, the metering capability usage should look like a simple dot notation
    call on some object



## Extensibility
1. We want to be able to add new capabilities without breaking existing code.
2. We are comfortable assuming new capability addition is _internal_ to the metering
    context and not something that is done by external code. That is, capabailities are
    not library extensible, but rather are defined by the metering context itself for
    callers to use.
3. New MeteringContext structs can be added in the future
    that may or may not implement the same capabilities as existing ones, we want partial
    implementations to be possible and easy. Without stable specialization / defaults
    this is not possible, so we use a trait-based approach to define capabilities. We'd like
    implementors of a MeteringContext to be able to see only the capabilities
    they care about, and not have to implement all of them.
4. We assume new capabilities are added somewhat infrequently, so some boilerplate
    is acceptable for the sake of clarity and type safety.

The general framework here is you have MeteringContext structs{}
that may have different capabilities that a given MeteringContext
may or may not implement.

Alternatives Considered:
    - Using an enum to represent capabilities, this is clunky
    - Using a vtable like approach with trait objects, this is complicated
    - Using different casting hacks, this is difficult to maintain, but also may
    rely on unsafe of unstable features, or outside crates that rely on these.
      e.g
      if (*self).type_id() == TypeId::of::<T>() {
            Ok(unsafe { &mut *(self as *mut dyn IsAnEvent as *mut T) })
      }

Notes for melding this with exisitng code:
    - This does not yet account for the design of our current Metering Events, which is a
    single enum. We need to think about if we want to keep that or amend it to something else.

Current gaps
    - The as_* name is a bit clunky, but it is the best we can do without any sort of support for
        creating the name in the macro
    - Ideally we'd use proc-macros since #[] is cleaner than `declare_event_capabilities!`
    - impl_dyn_event_forwarders! requires verbosely specifying the methods.


How to implement a new capability:

1. Define a new trait for the capability, e.g. `trait MyNewCapability`.
2. Add a blanket implementation of `impl_dyn_event_forwarders!` for the new trait.
3. Add the new trait to the `MeteringContext` trait using `declare_event_capabilities!`.

**/

/////////// MeteringContext trait ///////////
trait MeteringContext: AsAny {
    // Declare all the capabilities that metering contexts can implement.
    // When adding a new capability, you should add it here
    declare_event_capabilities!(
        AddRequestStartTime => as_add_request_start_time,
        LogCacheAccess => as_log_cache_access
    );
}

/////////////// Example Capability Implementation ///////////////
trait AddRequestStartTime {
    fn add_request_start_time(&mut self, start_time: std::time::Instant);
}

impl_dyn_event_forwarders!(
    AddRequestStartTime => as_add_request_start_time {
        fn add_request_start_time(&mut self, start_time: std::time::Instant);
    }
);

trait LogCacheAccess {
    // A cache hit occured
    fn cache_hit(&mut self);
    // A cache miss occured
    fn cache_miss(&mut self);
}

impl_dyn_event_forwarders!(
    LogCacheAccess => as_log_cache_access {
        fn cache_hit(&mut self);
        fn cache_miss(&mut self);
    }
);

///////////////////// Example MeteringContext Implementation //////////////////
struct AMeteringContext {
    misc: String, // example of a non-editable field that is base to the event and isn't for the Capability
    request_start_time: Option<std::time::Instant>, // NOTE!! This could be packaged in a subtype that different contexts share
    some_data: Option<String>,
}

impl AMeteringContext {
    fn new(misc: String) -> Self {
        AMeteringContext {
            misc,
            request_start_time: None,
            some_data: None,
        }
    }
}

impl AddRequestStartTime for AMeteringContext {
    fn add_request_start_time(&mut self, start_time: std::time::Instant) {
        self.request_start_time = Some(start_time);
    }
}
impl_event_capabilities!(AMeteringContext, {
    AddRequestStartTime => as_add_request_start_time,
});

//////// Example of the event stack just to show how it can be used with the MeteringContext trait ////////
struct EventStack {
    events: Vec<Box<dyn MeteringContext>>,
}

impl EventStack {
    fn new() -> Self {
        EventStack { events: Vec::new() }
    }

    fn push_event(&mut self, event: Box<dyn MeteringContext>) {
        self.events.push(event);
    }

    fn pop_event(&mut self) -> Option<Box<dyn MeteringContext>> {
        self.events.pop()
    }

    fn peek_event(&mut self) -> Option<&mut Box<dyn MeteringContext>> {
        self.events.last_mut()
    }

    fn finalize_event<Target>(&mut self) -> Result<Target, String>
    where
        Target: MeteringContext,
    {
        if let Some(event) = self.events.pop() {
            if let Ok(target_event) = event.as_any_box().downcast::<Target>() {
                return Ok(*target_event);
            } else {
                return Err("Event type mismatch".to_string());
            }
        }
        Err("No event to finalize".to_string())
    }
}

// Badly named trait to represent an event that can be any'ed
trait AsAny: Any {
    fn as_any_box(self: Box<Self>) -> Box<dyn Any>;
}
impl<T: Any> AsAny for T {
    fn as_any_box(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example() {
        let mut event_stack = EventStack::new();

        // Create an event and push it onto the stack
        let event =
            Box::new(AMeteringContext::new("Initial data".to_string())) as Box<dyn MeteringContext>;
        event_stack.push_event(event);

        // SAFETY(hammadb): We know there is one event on the stack
        let peeked = event_stack.peek_event().unwrap();
        peeked.add_request_start_time(std::time::Instant::now());
        peeked.cache_hit(); // This is not supported but we can call it and it should no-op

        let finalized_event: Result<AMeteringContext, String> = event_stack.finalize_event();
        match finalized_event {
            Ok(event) => {
                assert_eq!(event.misc, "Initial data");
                assert!(event.request_start_time.is_some());
                assert!(event.some_data.is_none());
            }
            Err(err) => panic!("Failed to finalize event: {}", err),
        }
    }
}
