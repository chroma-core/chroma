chroma_metering::initialize_metering! {
    #[attribute(name = "example_my_attribute")]
    type ExampleMyAttribute = Option<u8>;

    #[event]
    #[derive(Debug, Default, Clone)]
    pub struct MyMeteringEventExample {
        test_constant_field: Option<u8>,
        #[field(attribute = "example_my_attribute", mutator = "my_mutator")]
        test_annotated_field: Option<u8>,
    }
}

fn my_mutator(event: &mut MyMeteringEventExample, value: Option<u8>) {
    event.test_annotated_field = value;
}

pub fn main() {
    let mut test_event = MyMeteringEventExample::new(Some(100));
    println!("{:?}", test_event);
    println!("test_constant_field {:?}", test_event.test_constant_field);
    test_event.example_my_attribute(Some(50));
    println!("{:?}", test_event);
    test_event.example_my_attribute(Some(100));
    println!("{:?}", test_event);
}
