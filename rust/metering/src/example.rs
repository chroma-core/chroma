use std::any::Any;

// Example of an event that can be mutated and has a request start time
trait AddRequestStartTime {
    fn add_request_start_time(&mut self, start_time: std::time::Instant);
}

// Badly named trait to represent an event that can be any'ed
trait AsAny: Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any_owned(self: Box<Self>) -> Box<dyn Any>;
}

// Blanket implementation for any type that implements `Any`
impl<T: Any> AsAny for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_owned(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

trait IsAnEvent: AsAny {
    // not ideal that this has to be impl'ed
    fn as_add_request_start_time(&mut self) -> Result<&mut dyn AddRequestStartTime, String> {
        Err("This event does not support adding request start time".to_string())
    }
}

// Example of an event
struct AEvent {
    misc: String, // example of a non-editable field that is base to the event and doesn't come from the mutators
    request_start_time: Option<std::time::Instant>,
    some_data: Option<String>,
}

impl AEvent {
    fn new(misc: String) -> Self {
        AEvent {
            misc,
            request_start_time: None,
            some_data: None,
        }
    }
}

impl AddRequestStartTime for AEvent {
    fn add_request_start_time(&mut self, start_time: std::time::Instant) {
        self.request_start_time = Some(start_time);
    }
}

impl IsAnEvent for AEvent {
    fn as_add_request_start_time(&mut self) -> Result<&mut dyn AddRequestStartTime, String> {
        Ok(self as &mut dyn AddRequestStartTime)
    }
}

// Blanket implementation for the trait to allow downcasting
impl AddRequestStartTime for dyn IsAnEvent {
    fn add_request_start_time(&mut self, start_time: std::time::Instant) {
        if let Ok(event) = self.as_add_request_start_time() {
            event.add_request_start_time(start_time);
        } else {
            panic!("Event does not support adding request start time");
        }
    }
}

struct AnEventStack {
    events: Vec<Box<dyn IsAnEvent>>,
}

impl AnEventStack {
    fn new() -> Self {
        AnEventStack { events: Vec::new() }
    }

    fn push_event(&mut self, event: Box<dyn IsAnEvent>) {
        self.events.push(event);
    }

    fn pop_event(&mut self) -> Option<Box<dyn IsAnEvent>> {
        self.events.pop()
    }

    fn peek_event(&mut self) -> Option<&mut Box<dyn IsAnEvent>> {
        self.events.last_mut()
    }

    fn finalize_event<Target>(&mut self) -> Result<Target, String>
    where
        Target: IsAnEvent,
    {
        if let Some(event) = self.events.pop() {
            if let Ok(target_event) = event.as_any_owned().downcast::<Target>() {
                return Ok(*target_event);
            } else {
                return Err("Event type mismatch".to_string());
            }
        }
        Err("No event to finalize".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example() {
        let mut event_stack = AnEventStack::new();

        // Create an event and push it onto the stack
        let mut event = Box::new(AEvent::new("Initial data".to_string())) as Box<dyn IsAnEvent>;
        event_stack.push_event(event);

        // SAFETY(hammadb): We know there is one event on the stack
        let peeked = event_stack.peek_event().unwrap();

        peeked.add_request_start_time(std::time::Instant::now());

        let finalized_event: Result<AEvent, String> = event_stack.finalize_event();
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
