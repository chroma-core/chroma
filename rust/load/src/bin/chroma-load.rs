fn main() {
    println!("moo!");
    // Stop once per day.
    std::thread::sleep(std::time::Duration::from_secs(86_400));
    println!("going down!");
}
