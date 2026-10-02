//! Optional CUDA scoring bridge for the benchmark reader.

use std::sync::OnceLock;

type ScoreFn = unsafe extern "C" fn(
    *const *const u8,
    *const i32,
    *const u8,
    *const i32,
    *const f32,
    *mut f32,
    i32,
    i32,
    *mut f32,
) -> i32;

static SCORE_FUNCTION: OnceLock<ScoreFn> = OnceLock::new();

pub fn enabled() -> bool {
    std::env::var_os("HSPANN_GPU_LIBRARY").is_some()
}

pub fn score(
    code_ptrs: &[*const u8],
    code_counts: &[i32],
    planes: &[u8],
    leaf_indices: &[i32],
    params: &[f32],
) -> (Vec<f32>, [f32; 3]) {
    assert_eq!(code_ptrs.len(), code_counts.len());
    assert_eq!(
        code_counts.iter().map(|&n| n as usize).sum::<usize>(),
        leaf_indices.len()
    );
    assert_eq!(planes.len() % 512, 0);
    assert_eq!(params.len(), (planes.len() / 512) * 6);
    let count = i32::try_from(leaf_indices.len()).expect("too many GPU codes");
    let leaves = i32::try_from(planes.len() / 512).expect("too many GPU leaves");
    let function = *SCORE_FUNCTION.get_or_init(|| {
        let path = std::env::var_os("HSPANN_GPU_LIBRARY")
            .expect("HSPANN_GPU_LIBRARY must name the compiled CUDA library");
        let library =
            unsafe { libloading::Library::new(path) }.expect("failed to open CUDA scoring library");
        let function = unsafe {
            *library
                .get::<ScoreFn>(b"hspann_score_codes\0")
                .expect("CUDA scoring function is missing")
        };
        std::mem::forget(library);
        function
    });
    let mut output = vec![0.0f32; leaf_indices.len()];
    let mut diagnostics = [0.0f32; 3];
    let status = unsafe {
        function(
            code_ptrs.as_ptr(),
            code_counts.as_ptr(),
            planes.as_ptr(),
            leaf_indices.as_ptr(),
            params.as_ptr(),
            output.as_mut_ptr(),
            count,
            leaves,
            diagnostics.as_mut_ptr(),
        )
    };
    assert_eq!(status, 0, "CUDA scoring failed with status {status}");
    (output, diagnostics)
}
