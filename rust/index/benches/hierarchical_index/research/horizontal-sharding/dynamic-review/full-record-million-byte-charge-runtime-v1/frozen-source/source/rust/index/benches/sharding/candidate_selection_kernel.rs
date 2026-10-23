//! Optimized standalone measurement of the exact reader selection helper.
#[path="../hierarchical_index/reader/selection.rs"]
mod selection;
use std::{hint::black_box,time::Instant};
fn normalize(rows:&[(u32,f32)])->Vec<(u32,u32)>{let mut values:Vec<_>=rows.iter().map(|(id,d)|(*id,d.to_bits())).collect();values.sort();values}
fn main(){
    let args:Vec<_>=std::env::args().collect();assert_eq!(args.len(),2);
    let sizes:Vec<usize>=args[1].split(',').map(|n|n.parse().unwrap()).collect();assert!(!sizes.is_empty() && sizes.len()<=8 && sizes.iter().all(|n|*n<=200000));
    for size in sizes {for shape in ["generated-spread","generated-tie-heavy","all-ties"] {
        let mut state=0x7e57u64;let mut input=Vec::with_capacity(size);
        for id in 0..size {state=state.wrapping_mul(6364136223846793005).wrapping_add(1);let value=match shape {"all-ties"=>if id%2==0{0.}else{-0.},"generated-tie-heavy"=>(state%32) as f32,_=>(state%1000003) as f32-500001.};input.push((id as u32,value));}
        // Reverse each pair order on alternating repeats. Generation, copies,
        // normalization and assertions are deliberately outside the kernel.
        for repeat in 0..7 {let order=if repeat%2==0{[false,true]}else{[true,false]};let mut outputs=Vec::new();
            for partition in order {let clone_start=Instant::now();let mut rows=input.clone();let clone_ns=clone_start.elapsed().as_nanos();
                let start=Instant::now();let work=selection::select_candidates(black_box(&mut rows),1000,partition).unwrap();let elapsed=start.elapsed().as_nanos();black_box(&rows);
                println!("{{\"size\":{},\"shape\":\"{}\",\"repeat\":{},\"partition\":{},\"kernel_ns\":{},\"clone_ns\":{},\"sorted_rows\":{},\"retained_rows\":{},\"partition_calls\":{},\"select_ns\":{},\"sort_ns\":{}}}",size,shape,repeat,partition,elapsed,clone_ns,work.sorted_rows,work.retained_rows,work.partition_calls,work.select_ns,work.sort_ns);
                outputs.push(normalize(&rows));}
            assert_eq!(outputs[0],outputs[1]);
        }
    }}
}
