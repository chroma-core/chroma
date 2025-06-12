initialize_metering! {
    #[allow(type_complexity)] // we might have foreign macros before the subscription annotation
    #[subscription(id = "some_subscription")] // the subscription annotation must be of this form
    #[serde(rename = "snake_case")] // we may also have foreign macros after the subscription annotation
    pub trait SomeSubscription { // note that pub can be present or not, and we need to handle pub(crate), pub(super)
        fn an_event_happened(
            &self, // a reference to self *must be the first argument.
            arg_1: u8, // the method may have as many other parameters as desired (can be zero)
            arg2: Arc<std::sync::Mutex<Option<String>>> // these parameters' and their types can be of arbitrary complexity
        );
        // exactly one method must be in a subscription, no more, no less
    }

    #[derive(Debug, Clone)] // contexts may also have foreign macros before or after their annotation, or both, or neither.
    #[context( // context annotations must be of this form. the subscription and handler arrays must be of equal length
        subscriptions = ["some_subscription"],
        handlers = ["some_handler"]
    )]
    #[serde(skip = "true")] // an example of a foreign macro after.
    struct SomeContext { // contexts may or may not have a visibility modifier
        pub my_field: u8, // fields may also have visibility modifiers
        my_other_field: Arc<std::sync::Mutex<>> // fields may have types of arbitrary complexity
    }
}

// handlers are defined outside of the macro so we don't have to worry about them. we get their idents from
// the annotations
fn some_handler(context: &SomeContext, v1: u8, v2: Arc<std::sync::Mutex<Option<String>>>) {}
