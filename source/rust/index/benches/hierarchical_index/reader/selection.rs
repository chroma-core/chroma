//! Preserve every numeric cutoff tie while sorting only retained candidates.
use std::time::Instant;
#[derive(Clone,Debug,Default)]
pub struct SelectionWork {
    pub input_rows:usize,pub sorted_rows:usize,pub retained_rows:usize,
    pub partition_calls:usize,pub select_ns:u64,pub sort_ns:u64,
}
fn ns(start:Instant)->u64 {start.elapsed().as_nanos().min(u64::MAX as u128) as u64}

/// Input is already deduplicated by the existing minimum-distance rule.
/// Numeric comparisons group signed zeros; callers apply external-ID ordering
/// after mapping all retained ties. Equal-distance ID order is unspecified.
pub fn select_candidates(rows:&mut Vec<(u32,f32)>,budget:usize,partition:bool)->Result<SelectionWork,String> {
    if budget==0 || rows.iter().any(|(_,d)|!d.is_finite()){return Err("positive budget and finite candidate distances required".into());}
    let mut work=SelectionWork{input_rows:rows.len(),..Default::default()};
    if partition && rows.len()>budget {
        let start=Instant::now();
        let boundary=rows.select_nth_unstable_by(budget-1,|a,b|a.1.partial_cmp(&b.1).unwrap()).1.1;
        rows.retain(|(_,distance)|*distance<=boundary);
        work.partition_calls=1;work.select_ns=ns(start);
    }
    work.sorted_rows=rows.len();let start=Instant::now();
    rows.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());work.sort_ns=ns(start);
    if !partition && rows.len()>budget {
        let start=Instant::now();let boundary=rows[budget-1].1;
        let keep=rows.iter().take_while(|(_,d)|*d<=boundary).count();rows.truncate(keep);work.select_ns=ns(start);
    }
    work.retained_rows=rows.len();Ok(work)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn normalized(rows:&[(u32,f32)])->Vec<(u32,u32)>{let mut v:Vec<_>=rows.iter().map(|(i,d)|(*i,d.to_bits())).collect();v.sort();v}
    fn check(input:Vec<(u32,f32)>,m:usize){let mut a=input.clone();let mut b=input;let old=select_candidates(&mut a,m,false).unwrap();let new=select_candidates(&mut b,m,true).unwrap();assert_eq!(normalized(&a),normalized(&b));assert_eq!(old.retained_rows,new.retained_rows);assert!(new.sorted_rows<=old.sorted_rows);}
    #[test]
    fn edges_ties_signed_zero_negative_and_extremes(){
        for input in [vec![],vec![(0,1.)],vec![(0,0.),(1,-0.),(2,1.),(3,-1.)],vec![(0,f32::MAX),(1,-f32::MAX),(2,f32::MIN_POSITIVE)],(0..20).map(|id|(id,1.)).collect()] {
            for m in 1..=input.len()+2 {check(input.clone(),m);}
        }
    }
    #[test]
    fn differential_permutations_and_every_budget(){
        let mut state=19u64;
        for n in 0..80 {let mut rows=Vec::new();for id in 0..n {state=state.wrapping_mul(6364136223846793005).wrapping_add(1);rows.push((id,(state%13) as f32-6.));}
            for _ in 0..3 {for i in (1..rows.len()).rev(){state=state.wrapping_mul(6364136223846793005).wrapping_add(1);rows.swap(i,state as usize%(i+1));}for m in 1..=n as usize+1{check(rows.clone(),m);}}
        }
    }
    #[test]
    fn duplicate_minima_keep_first_equal_bits_before_selection(){
        let copies=vec![(0,0.),(0,-0.),(1,5.),(1,2.),(2,2.),(2,7.)];let mut best=std::collections::HashMap::new();
        for (id,d) in copies{let old=best.entry(id).or_insert(d);if d<*old{*old=d;}}
        assert_eq!(best[&0].to_bits(),0f32.to_bits());assert_eq!(best[&1],2.);check(best.into_iter().collect(),2);
    }
    #[test]
    fn invalid_budget_or_nonfinite_rejects_before_reordering(){
        for value in [f32::NAN,f32::INFINITY,f32::NEG_INFINITY] {for mode in [false,true]{let mut rows=vec![(0,2.),(1,value)];let before=normalized(&rows);assert!(select_candidates(&mut rows,1,mode).is_err());assert_eq!(normalized(&rows),before);}}
        assert!(select_candidates(&mut vec![],0,true).is_err());
    }
}
