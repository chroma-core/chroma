pub mod hir;
pub mod literal_expr;

use hir::Hir;
use regex::Regex;
use regex_syntax::parse;
use thiserror::Error;

/// Chroma custom wrapper for a Regex pattern.
///
/// The `ChromaRegex` struct should represent a regex pattern that is supported by Chroma, and
/// it could be safely used by further implementations. During construction of this struct we
/// will perform all necessary validation to check this.
///
/// We would like to leverage the regex_syntax crate to provide basic parsing and simplification
/// of regex expression, and in the process guard against unsupported features by examing the
/// parsed syntax tree.
#[derive(Clone, Debug)]
pub struct ChromaRegex {
    hir: Hir,
    pattern: String,
}

#[derive(Debug, Error)]
pub enum ChromaRegexError {
    #[error("Byte pattern is not allowed")]
    BytePattern,
    #[error("Pattern that always matches is not allowed")]
    EmptyPattern,
    // NOTE: regex::Error is a large type, so we only store its error message here.
    #[error("Unexpected regex error: {0}")]
    Regex(String),
    // NOTE: regex_syntax::Error is a large type, so we only store its error message here.
    #[error("Regex syntax errror: {0}")]
    RegexSyntax(String),
}

impl ChromaRegex {
    pub fn hir(&self) -> &Hir {
        &self.hir
    }
    pub fn regex(&self) -> Result<Regex, ChromaRegexError> {
        // NOTE: Although this method return a Result<_, _> type, in practice it should always
        // be Ok(_) becasue we validate the pattern during struct construction. Specifically,
        // we verify that the pattern can be properly parsed and is thus a valid pattern supported
        // by the regex crate.
        Regex::new(&self.pattern).map_err(|e| ChromaRegexError::Regex(e.to_string()))
    }
}

impl TryFrom<String> for ChromaRegex {
    type Error = ChromaRegexError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let hir = parse(&value).map_err(|e| ChromaRegexError::RegexSyntax(e.to_string()))?;
        if let Some(0) = hir.properties().minimum_len() {
            return Err(ChromaRegexError::EmptyPattern);
        }
        let chroma_hir = Hir::try_from(hir)?;
        if let Hir::Empty = chroma_hir {
            return Err(ChromaRegexError::EmptyPattern);
        }
        Ok(Self {
            hir: chroma_hir,
            pattern: value,
        })
    }
}
