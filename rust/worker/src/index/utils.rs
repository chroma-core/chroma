use rand::Rng;

pub(super) fn generate_random_data(n: usize, d: usize) -> Vec<f32> {
    let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
    let mut data = vec![0.0f32; n * d];
    // Generate random data
    for i in 0..n {
        for j in 0..d {
            data[i * d + j] = rng.gen();
        }
    }
    return data;
}
