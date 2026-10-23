use std::{hint::black_box, time::Instant};
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_distance::DistanceFunction;
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
unsafe fn popcnt_probe(codes: &[u8], qq: &QuantizedQuery, output: &mut [f32]) {
    for (bytes, score) in codes.chunks_exact(Code::<1>::size(1024)).zip(output) {
        *score = Code::<1,_>::new(bytes).distance_quantized_query(&DistanceFunction::Euclidean, qq);
    }
}
fn main() {
    let dim = 1024;
    let n = 4096;
    let centroid: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.31).sin()).collect();
    let query: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.73).cos()).collect();
    let qq = QuantizedQuery::new(&query, Code::<1>::packed_len(dim), 1.0, 2.0, 3.0);
    let codes: Vec<u8> = (0..n).flat_map(|v| {
        let embedding: Vec<f32> = (0..dim).map(|i| ((i + v) as f32 * 0.57).sin()).collect();
        Code::<1>::quantize(&embedding, &centroid).as_ref().to_vec()
    }).collect();
    let mut output = vec![0.0;n];
    for pass in 0..6 {
        for mode in if pass % 2 == 0 { [0,1,2] } else { [2,1,0] } {
            let t = Instant::now();
            for _ in 0..200 {
                if mode == 1 { Code::<1>::distances_quantized_query(black_box(&codes), &DistanceFunction::Euclidean, black_box(&qq), black_box(&mut output)); }
                else if mode == 2 {
                    #[cfg(target_arch = "x86_64")] unsafe { popcnt_probe(black_box(&codes), black_box(&qq), black_box(&mut output)); }
                }
                else { for (bytes, score) in codes.chunks_exact(Code::<1>::size(dim)).zip(&mut output) { *score = Code::<1,_>::new(black_box(bytes)).distance_quantized_query_reference(&DistanceFunction::Euclidean, black_box(&qq)); } }
                black_box(&output);
            }
            println!("pass={pass} mode={mode} ns/code={} checksum={}",t.elapsed().as_nanos() as f64/(200*n) as f64,output.iter().sum::<f32>());
        }
    }
}
