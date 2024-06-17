pub(crate) fn merge_sorted_vecs_disjunction<T: Ord + Clone>(a: &Vec<T>, b: &Vec<T>) -> Vec<T> {
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
        if a_val < b_val {
            result.push(a_val.clone());
            a_idx += 1;
        } else if a_val > b_val {
            result.push(b_val.clone());
            b_idx += 1;
        } else {
            result.push(a_val.clone());
            a_idx += 1;
            b_idx += 1;
        }
    }
    result
}

pub(crate) fn merge_sorted_vecs_conjunction<T: Ord + Clone>(a: &Vec<T>, b: &Vec<T>) -> Vec<T> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut a_idx = 0;
    let mut b_idx = 0;
    while a_idx < a.len() && b_idx < b.len() {
        let a_val = &a[a_idx];
        let b_val = &b[b_idx];
        if a_val < b_val {
            a_idx += 1;
        } else if a_val > b_val {
            b_idx += 1;
        } else {
            result.push(a_val.clone());
            a_idx += 1;
            b_idx += 1;
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
