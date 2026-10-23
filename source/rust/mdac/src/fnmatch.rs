//! Perform fnmatch on text, returning true if it matches pattern.

// NOTE(rescrv):  This fnmatch implementation was borrowed from
// https://github.com/rescrv/blue/blob/7b6d16bdcd9ff2f53a7426c6ad67802399124237/utilz/src/fnmatch.rs
// The license is Apache 2.0.
// The original author is also the one doing the borrowing and the borrowing is allowed by the license.

fn fnmatch(pattern: &str, text: &str) -> bool {
    let mut pat = pattern.chars();
    let mut txt = text.chars();
    'processing: loop {
        match (pat.next(), txt.next()) {
            (Some(p), Some(t)) => {
                if p == '*' {
                    for (idx, _) in text.char_indices() {
                        if fnmatch(&pattern[1..], &text[idx..]) {
                            return true;
                        }
                    }
                    continue 'processing;
                } else if p == t {
                    continue 'processing;
                } else {
                    return false;
                }
            }
            (Some(p), None) => p == '*' && pat.all(|c| c == '*'),
            (None, Some(_)) => {
                return false;
            }
            (None, None) => {
                return true;
            }
        };
    }
}

////////////////////////////////////////////// Pattern /////////////////////////////////////////////

/// A [Pattern] captures the pattern for globbing.  Call `fnmatch` to check if a text string
/// matches.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Pattern {
    pattern: String,
}

impl Pattern {
    /// True if and only if the pattern is valid.
    ///
    /// Currently just checks that the pattern is less than 64 characters long.  An arbitrary
    /// restriction because recursion.
    pub fn is_valid(pattern: &str) -> bool {
        pattern.len() < 64
    }

    /// Panics on an invalid pattern.
    pub fn must(pattern: impl Into<String>) -> Self {
        Self::new(pattern).expect("invalid pattern in a must declaration")
    }

    /// Create a new pattern if it is valid; else None.
    pub fn new(pattern: impl Into<String>) -> Option<Self> {
        let pattern = pattern.into();
        Pattern::is_valid(&pattern).then_some(Self { pattern })
    }

    /// Run fnmatch of the pattern against `text`.
    pub fn fnmatch(&self, text: &str) -> bool {
        fnmatch(&self.pattern, text)
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        assert!(Pattern::must("".to_owned()).fnmatch(""));
        assert!(!Pattern::must("".to_owned()).fnmatch("abc"));
        assert!(Pattern::must("abc".to_owned()).fnmatch("abc"));
        assert!(Pattern::must("a*c".to_owned()).fnmatch("abc"));
        assert!(Pattern::must("a*c".to_owned()).fnmatch("aabbcc"));
        assert!(Pattern::must("*bc".to_owned()).fnmatch("abc"));
        assert!(Pattern::must("*bc".to_owned()).fnmatch("bc"));
        assert!(Pattern::must("ab*".to_owned()).fnmatch("abc"));
        assert!(Pattern::must("ab*".to_owned()).fnmatch("ab"));
    }
}
