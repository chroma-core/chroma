///// Macros ///////

#[macro_export]
macro_rules! declare_event_capabilities {
    // Intended to expand inside the definition of `IsAnEvent`
    //
    //     declare_event_capabilities! {
    //         AddRequestStartTime  => as_add_request_start_time,
    //         AddFooBar           => as_add_foo_bar,
    //     }
    //
    ( $( $cap_trait:path => $as_fn:ident ),* $(,)? ) => {
        $(
            fn $as_fn(&mut self) -> Result<&mut dyn $cap_trait, String> {
                Err(format!(
                    "This event does not support {}",
                    stringify!($cap_trait)
                ))
            }
        )*
    };
}

#[macro_export]
macro_rules! impl_event_capabilities {
    // Generate one `impl IsAnEvent for $event` with overrides for every
    // capability the event chooses to opt-in to.
    //
    //     impl_event_capabilities!(MyEvent, {
    //         AddRequestStartTime => as_add_request_start_time,
    //         AddFooBar           => as_add_foo_bar,
    //     });
    //
    ($event:ty, { $( $cap_trait:path => $as_fn:ident ),* $(,)? }) => {
        impl MeteringContext for $event {
            $(
                fn $as_fn(&mut self) -> Result<&mut dyn $cap_trait, String> {
                    Ok(self as &mut dyn $cap_trait)
                }
            )*
        }
    };
}

#[macro_export]
macro_rules! impl_dyn_event_forwarders {
    (
        $(
            $cap:path => $accessor:ident {
                $(
                    fn $m:ident ( &mut self $( , $arg:ident : $ty:ty )* ) $( -> $ret:ty )? ;
                )*
            }
        ),* $(,)?
    ) => {
        $(
            impl $cap for dyn MeteringContext {
                $(
                    fn $m(&mut self $( , $arg : $ty )* ) $( -> $ret )? {
                        match self.$accessor() {
                            Ok(inner) => inner.$m($($arg),*),
                            Err(_) => {
                                #[allow(unreachable_code)]
                                {
                                    $( < $ret as Default >::default() )?
                                }
                            }
                        }
                    }
                )*
            }
        )*
    };
}
