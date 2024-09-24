#[cfg(test)]
use rand::Rng;

#[cfg(test)]
pub(super) fn generate_random_data(n: usize, d: usize) -> Vec<f32> {
    let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
    let mut data = vec![0.0f32; n * d];
    // Generate random data
    for i in 0..n {
        for j in 0..d {
            data[i * d + j] = rng.gen();
        }
    }
    data
}

pub fn merge_sorted_vecs_disjunction<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut a_idx = 0;
    let mut b_idx = 0;
    while a_idx < a.len() || b_idx < b.len() {
        if a_idx == a.len() {
            result.push(b[b_idx].clone());
            b_idx += 1;
            continue;
        }
        if b_idx == b.len() {
            result.push(a[a_idx].clone());
            a_idx += 1;
            continue;
        }

        let a_val = &a[a_idx];
        let b_val = &b[b_idx];
        match a_val.cmp(b_val) {
            std::cmp::Ordering::Less => {
                result.push(a_val.clone());
                a_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(b_val.clone());
                b_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a_val.clone());
                a_idx += 1;
                b_idx += 1;
            }
        }
    }
    result
}

pub fn merge_sorted_vecs_conjunction<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut a_idx = 0;
    let mut b_idx = 0;
    while a_idx < a.len() && b_idx < b.len() {
        let a_val = &a[a_idx];
        let b_val = &b[b_idx];
        match a_val.cmp(b_val) {
            std::cmp::Ordering::Less => {
                a_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                b_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a_val.clone());
                a_idx += 1;
                b_idx += 1;
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_merge_sorted_vecs_disjunction_rhs_empty() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![];
        let result = super::merge_sorted_vecs_disjunction(&a, &b);
        assert_eq!(result, vec![1, 3, 5, 7, 9]);
    }

    #[test]
    fn test_merge_sorted_vecs_disjunction_lhs_empty() {
        let a = vec![];
        let b = vec![2, 4, 6, 8, 10];
        let result = super::merge_sorted_vecs_disjunction(&a, &b);
        assert_eq!(result, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_merge_sorted_vecs_disjunction_both_empty() {
        let a: Vec<i32> = vec![];
        let b: Vec<i32> = vec![];
        let result = super::merge_sorted_vecs_disjunction(&a, &b);
        assert!(result.is_empty())
    }

    #[test]
    fn test_merge_sorted_vecs_disjunction_both_populated() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![2, 4, 6, 8, 10];
        let result = super::merge_sorted_vecs_disjunction(&a, &b);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_merge_sorted_vecs_disjunction_lhs_subset() {
        let a = vec![1, 3, 5];
        let b = vec![2, 4, 6, 8, 10];
        let result = super::merge_sorted_vecs_disjunction(&a, &b);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 8, 10]);
    }

    #[test]
    fn test_merge_sorted_vecs_conjunct_both_empty() {
        let a: Vec<i32> = vec![];
        let b: Vec<i32> = vec![];
        let result = super::merge_sorted_vecs_conjunction(&a, &b);
        assert!(result.is_empty())
    }

    #[test]
    fn test_merge_sorted_vecs_conjunct_lhs_empty() {
        let a = vec![];
        let b = vec![2, 4, 6, 8, 10];
        let result = super::merge_sorted_vecs_conjunction(&a, &b);
        assert!(result.is_empty())
    }

    #[test]
    fn test_merge_sorted_vecs_conjunct_rhs_empty() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![];
        let result = super::merge_sorted_vecs_conjunction(&a, &b);
        assert!(result.is_empty())
    }

    #[test]
    fn test_merge_sorted_vecs_conjunct_both_populated() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![2, 3, 5, 8, 10];
        let result = super::merge_sorted_vecs_conjunction(&a, &b);
        assert_eq!(result, vec![3, 5]);
    }

    #[test]
    fn test_merge_sorted_vecs_conjunct_lhs_subset() {
        let a = vec![1, 3, 5];
        let b = vec![1, 2, 3, 5, 8, 10];
        let result = super::merge_sorted_vecs_conjunction(&a, &b);
        assert_eq!(result, vec![1, 3, 5]);
    }

    #[test]
    fn test_merge_sorted_vecs_conjunct_rhs_subset() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![1, 2, 3, 5];
        let result = super::merge_sorted_vecs_conjunction(&a, &b);
        assert_eq!(result, vec![1, 3, 5]);
    }
}
