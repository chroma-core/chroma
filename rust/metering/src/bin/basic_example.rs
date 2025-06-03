#[chroma_metering::attribute(name = "example_my_attribute")]
type ExampleMyAttribute = Option<u8>;

#[chroma_metering::event]
struct MyMeteringEventExample {
    test_constant_field: Option<u8>,
    #[field(attribute = "example_my_attribute", mutator = "my_mutator")]
    test_annotated_field: Option<u8>,
}

fn my_mutator(event: &mut MyMeteringEventExample, value: Option<u8>) {
    event.test_annotated_field = value;
}

#[chroma_metering::event]
struct MyOtherMeteringEventExample {
    other_test_constant_field: Option<u8>,
    #[field(attribute = "example_my_attribute", mutator = "my_other_mutator")]
    other_test_annotated_field: Option<u8>,
}

fn my_other_mutator(event: &mut MyOtherMeteringEventExample, value: Option<u8>) {
    if let Some(curr_value) = event.other_test_annotated_field.as_mut() {
        if let Some(inc) = value {
            *curr_value += inc;
        }
    }
}

pub fn main() {
    let mut test_event = MyMeteringEventExample::new(Some(100));
    println!("{:?}", test_event);
    test_event.example_my_attribute(Some(50));
    println!("{:?}", test_event);
    test_event.example_my_attribute(Some(100));
    println!("{:?}", test_event);
}
