fn main() {
    loop {
        println!("moo!");
        std::thread::sleep(std::time::Duration::from_secs(600));
    }
}
