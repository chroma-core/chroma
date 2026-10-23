//! Select an integer scoring kernel once for a complete posting batch.
//! The stored layout and floating-point correction remain unchanged.
use super::{Code, CodeHeader1Bit, QuantizedQuery};
use chroma_distance::DistanceFunction;
use std::mem::size_of;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OneBitScorerPolicy {
    /// Compile the ordinary count_ones kernel with the common build flags.
    Portable,
    /// Use hardware POPCNT only when runtime detection permits it.
    Runtime,
}
impl OneBitScorerPolicy {
    pub fn name(self) -> &'static str {
        match self { Self::Portable => "portable", Self::Runtime => "runtime" }
    }
}
#[derive(Clone, Copy, Debug)]
enum Implementation {
    Portable,
    #[cfg(target_arch = "x86_64")]
    Popcnt,
}
#[derive(Clone, Copy, Debug)]
pub struct OneBitScorer { policy: OneBitScorerPolicy, implementation: Implementation }

impl OneBitScorer {
    pub fn new(policy: OneBitScorerPolicy) -> Self {
        #[cfg(target_arch = "x86_64")]
        if policy == OneBitScorerPolicy::Runtime && std::is_x86_feature_detected!("popcnt") {
            return Self { policy, implementation: Implementation::Popcnt };
        }
        Self { policy, implementation: Implementation::Portable }
    }
    pub fn policy(self) -> OneBitScorerPolicy { self.policy }
    pub fn implementation_name(self) -> &'static str {
        match self.implementation {
            Implementation::Portable => "portable",
            #[cfg(target_arch = "x86_64")]
            Implementation::Popcnt => "x86_64-popcnt",
        }
    }
    /// Validate the entire batch before invoking the callback. The callback
    /// receives code order, including every physical copy and every tie.
    pub fn for_each_integer_sum(
        self, codes: &[u8], query: &QuantizedQuery, emit: impl FnMut(usize, u32),
    ) -> Result<usize, &'static str> {
        let pb = query.padded_bytes;
        let stride = size_of::<CodeHeader1Bit>().checked_add(pb).ok_or("code size overflow")?;
        if pb % 8 != 0 || pb.checked_mul(4) != Some(query.bit_planes.len())
            || pb.checked_mul(120).is_none_or(|v| v > u32::MAX as usize)
            || codes.len() % stride != 0 {
            return Err("invalid one-bit batch/plane length or integer range");
        }
        // Exactly one implementation branch for all codes in this leaf.
        match self.implementation {
            Implementation::Portable => portable_batch(codes, query, emit),
            #[cfg(target_arch = "x86_64")]
            Implementation::Popcnt => {
                // The private variant can only be constructed after detection.
                unsafe { popcnt_batch(codes, query, emit) }
            }
        }
        Ok(codes.len() / stride)
    }
    pub fn for_each_distance(
        self, codes: &[u8], query: &QuantizedQuery, distance: &DistanceFunction,
        mut emit: impl FnMut(usize, f32),
    ) -> Result<usize, &'static str> {
        let stride = size_of::<CodeHeader1Bit>().checked_add(query.padded_bytes).ok_or("code size overflow")?;
        self.for_each_integer_sum(codes, query, |i, sum| {
            let code = Code::<1, _>::new(&codes[i * stride..(i + 1) * stride]);
            emit(i, code.correct_integer_sum(distance, query, sum, query.c_norm, query.c_dot_q));
        })
    }
}

#[inline]
fn portable_sum(packed: &[u8], qq: &QuantizedQuery) -> u32 {
    let pb = qq.padded_bytes;
    let p0 = &qq.bit_planes[0..pb];
    let p1 = &qq.bit_planes[pb..2 * pb];
    let p2 = &qq.bit_planes[2 * pb..3 * pb];
    let p3 = &qq.bit_planes[3 * pb..4 * pb];
    let (mut pop0, mut pop1, mut pop2, mut pop3) = (0u32, 0u32, 0u32, 0u32);
    for (x_chunk, (((q0, q1), q2), q3)) in packed.chunks_exact(8).zip(
        p0.chunks_exact(8).zip(p1.chunks_exact(8)).zip(p2.chunks_exact(8)).zip(p3.chunks_exact(8)),
    ) {
        let x = u64::from_le_bytes(x_chunk.try_into().unwrap());
        pop0 += (x & u64::from_le_bytes(q0.try_into().unwrap())).count_ones();
        pop1 += (x & u64::from_le_bytes(q1.try_into().unwrap())).count_ones();
        pop2 += (x & u64::from_le_bytes(q2.try_into().unwrap())).count_ones();
        pop3 += (x & u64::from_le_bytes(q3.try_into().unwrap())).count_ones();
    }
    pop0 + (pop1 << 1) + (pop2 << 2) + (pop3 << 3)
}
fn portable_batch(codes: &[u8], qq: &QuantizedQuery, mut emit: impl FnMut(usize, u32)) {
    let header = size_of::<CodeHeader1Bit>();
    for (i, code) in codes.chunks_exact(header + qq.padded_bytes).enumerate() {
        emit(i, portable_sum(&code[header..], qq));
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
unsafe fn popcnt_batch(codes: &[u8], qq: &QuantizedQuery, mut emit: impl FnMut(usize, u32)) {
    use std::arch::x86_64::_popcnt64;
    let pb = qq.padded_bytes;
    let header = size_of::<CodeHeader1Bit>();
    let (p0, p1, p2, p3) = (&qq.bit_planes[..pb], &qq.bit_planes[pb..2 * pb],
        &qq.bit_planes[2 * pb..3 * pb], &qq.bit_planes[3 * pb..4 * pb]);
    for (index, code) in codes.chunks_exact(header + pb).enumerate() {
        let (mut a, mut b, mut c, mut d) = (0u32, 0u32, 0u32, 0u32);
        for (x_chunk, (((q0, q1), q2), q3)) in code[header..].chunks_exact(8).zip(
            p0.chunks_exact(8).zip(p1.chunks_exact(8)).zip(p2.chunks_exact(8)).zip(p3.chunks_exact(8)),
        ) {
            let x = u64::from_le_bytes(x_chunk.try_into().unwrap());
            // These intrinsics, and only these integer operations, need POPCNT.
            a += _popcnt64((x & u64::from_le_bytes(q0.try_into().unwrap())) as i64) as u32;
            b += _popcnt64((x & u64::from_le_bytes(q1.try_into().unwrap())) as i64) as u32;
            c += _popcnt64((x & u64::from_le_bytes(q2.try_into().unwrap())) as i64) as u32;
            d += _popcnt64((x & u64::from_le_bytes(q3.try_into().unwrap())) as i64) as u32;
        }
        emit(index, a + (b << 1) + (c << 2) + (d << 3));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quantization::rabitq_distance_query;
    fn qq(dim: usize) -> QuantizedQuery {
        let residual: Vec<_> = (0..dim).map(|i| if i % 11 == 0 { -0.0 } else { (i % 29) as f32 * 0.25 - 3.5 }).collect();
        QuantizedQuery::new(&residual, Code::<1>::packed_len(dim), 2., -0., 3.)
    }
    fn scalar_sum(packed: &[u8], q: &QuantizedQuery) -> u32 {
        let mut sum = 0;
        for (byte, &x) in packed.iter().enumerate() {
            for bit in 0..8 {
                if x & (1 << bit) != 0 {
                    for plane in 0..4 { sum += u32::from((q.bit_planes[plane * q.padded_bytes + byte] >> bit) & 1) << plane; }
                }
            }
        }
        sum
    }
    fn header(correction:f32,norm:f32,radial:f32,signed_sum:i32)->Vec<u8> {
        bytemuck::bytes_of(&CodeHeader1Bit{correction,norm,radial,signed_sum}).to_vec()
    }
    fn old_correction(bytes:&[u8], q:&QuantizedQuery, metric:&DistanceFunction)->f32 {
        let h:CodeHeader1Bit=bytemuck::pod_read_unaligned(&bytes[..16]);
        let packed_dot_qu=scalar_sum(&bytes[16..],q);
        let signed_sum=h.signed_sum as f32;
        let signed_dot_qu=2.0*packed_dot_qu as f32-q.sum_q_u as f32;
        let g_dot_r_q=0.5*(q.delta*signed_dot_qu+q.v_l*signed_sum);
        rabitq_distance_query(g_dot_r_q,h.correction,h.norm,h.radial,q.c_norm,q.c_dot_q,q.q_norm,metric)
    }
    #[test]
    fn hardware_scorer_integer_sums_match_scalar_bits_including_padding() {
        let mut state=0x7c159e3779b9u64;
        for dim in [1,7,8,9,63,64,65,127,128,129,300,768,769,1024,4096] {
            let mut q=qq(dim);
            // Include nonzero padding bits: the baseline scores all packed bytes.
            for b in &mut q.bit_planes {state^=state<<13;state^=state>>7;state^=state<<17;*b=state as u8;}
            let mut bytes=Vec::new();let mut expected=Vec::new();
            for index in 0..31 {
                let mut code=header(1.,1.,0.,0);
                for _ in 0..q.padded_bytes {state^=state<<13;state^=state>>7;state^=state<<17;code.push(match index {0=>0,1=>255,_=>state as u8});}
                expected.push(scalar_sum(&code[16..],&q));bytes.extend(code);
            }
            for policy in [OneBitScorerPolicy::Portable,OneBitScorerPolicy::Runtime] {
                let scorer=OneBitScorer::new(policy);let mut actual=Vec::new();
                assert_eq!(scorer.for_each_integer_sum(&bytes,&q,|i,sum|{assert_eq!(i,actual.len());actual.push(sum)}),Ok(31));
                assert_eq!(actual,expected,"dim={dim}, implementation={}",scorer.implementation_name());
                let mut unaligned=vec![0x55];unaligned.extend_from_slice(&bytes);let mut shifted=Vec::new();scorer.for_each_integer_sum(&unaligned[1..],&q,|_,sum|shifted.push(sum)).unwrap();assert_eq!(shifted,expected);
            }
        }
    }
    #[test]
    fn hardware_scorer_f32_bits_match_legacy_expression_and_code_method() {
        for dim in [1,7,64,65,300,768,769,4096] {
            let q=qq(dim);let centroid=vec![0.25;dim];let mut bytes=Vec::new();
            for index in 0..9 {let vector:Vec<_>=(0..dim).map(|i|if i%17==0{-0.}else{((i+index)%19) as f32*0.5-4.}).collect();bytes.extend_from_slice(Code::<1>::quantize(&vector,&centroid).as_ref());}
            for (correction,norm,radial,signed) in [(0.,0.,-0.,0),(-0.,-0.,0.,-1),(f32::MIN_POSITIVE,1.,-1.,i32::MIN),(1.,f32::MAX,0.,i32::MAX),(f32::INFINITY,1.,1.,7),(f32::from_bits(0x7fc00011),1.,0.,-3)] {
                bytes.extend(header(correction,norm,radial,signed));bytes.extend(vec![0xa5;q.padded_bytes]);
            }
            for metric in [DistanceFunction::Euclidean,DistanceFunction::Cosine,DistanceFunction::InnerProduct] {
                let expected:Vec<_>=bytes.chunks_exact(Code::<1>::size(dim)).map(|code|{
                    let old=old_correction(code,&q,&metric);
                    assert_eq!(Code::<1,_>::new(code).distance_quantized_query(&metric,&q).to_bits(),old.to_bits());
                    let custom=QuantizedQuery{bit_planes:q.bit_planes.clone(),padded_bytes:q.padded_bytes,v_l:q.v_l,delta:q.delta,sum_q_u:q.sum_q_u,c_norm:7.,c_dot_q:-2.,q_norm:q.q_norm};
                    assert_eq!(Code::<1,_>::new(code).distance_quantized_query2(&metric,&q,7.,-2.).to_bits(),old_correction(code,&custom,&metric).to_bits());old.to_bits()
                }).collect();
                for policy in [OneBitScorerPolicy::Portable,OneBitScorerPolicy::Runtime] {
                    let mut actual=Vec::new();OneBitScorer::new(policy).for_each_distance(&bytes,&q,&metric,|i,d|{assert_eq!(i,actual.len());actual.push(d.to_bits())}).unwrap();assert_eq!(actual,expected,"dim={dim}, metric={metric:?}");
                }
            }
        }
    }
    #[test]
    fn hardware_scorer_batch_shape_errors_precede_callbacks_and_empty_is_valid() {
        let q=qq(64);let scorer=OneBitScorer::new(OneBitScorerPolicy::Runtime);let mut calls=0;
        assert_eq!(scorer.for_each_distance(&[],&q,&DistanceFunction::Euclidean,|_,_|calls+=1),Ok(0));assert_eq!(calls,0);
        let code=vec![0;24];
        assert!(scorer.for_each_integer_sum(&code[..23],&q,|_,_|calls+=1).is_err());
        let mut malformed=qq(64);malformed.bit_planes.pop();assert!(scorer.for_each_integer_sum(&code,&malformed,|_,_|calls+=1).is_err());
        malformed.padded_bytes=usize::MAX;assert!(scorer.for_each_integer_sum(&[],&malformed,|_,_|calls+=1).is_err());
        malformed.padded_bytes=9;malformed.bit_planes=vec![0;36];assert!(scorer.for_each_integer_sum(&[],&malformed,|_,_|calls+=1).is_err());assert_eq!(calls,0);
        let mut count=0;assert_eq!(scorer.for_each_integer_sum(&code,&q,|i,_|{assert_eq!(i,0);count+=1}),Ok(1));assert_eq!(count,1);
    }
    #[test]
    fn hardware_scorer_policy_never_enters_unsupported_isa() {
        let portable=OneBitScorer::new(OneBitScorerPolicy::Portable);assert_eq!(portable.implementation_name(),"portable");assert_eq!(portable.policy().name(),"portable");
        let runtime=OneBitScorer::new(OneBitScorerPolicy::Runtime);assert_eq!(runtime.policy().name(),"runtime");
        #[cfg(target_arch="x86_64")]
        assert_eq!(runtime.implementation_name(),if std::is_x86_feature_detected!("popcnt"){"x86_64-popcnt"}else{"portable"});
        #[cfg(not(target_arch="x86_64"))]
        assert_eq!(runtime.implementation_name(),"portable");
        eprintln!("hardware scorer local architecture={} runtime_implementation={}",std::env::consts::ARCH,runtime.implementation_name());
    }
}
