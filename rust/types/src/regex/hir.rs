use regex_syntax::hir::{self, Class, ClassUnicode, HirKind, Repetition};

use super::ChromaRegexError;

/// Chroma custom internal representation for a Regex pattern.
///
/// This Hir is modeled from regex_syntax::HirKind, with some notable difference:
/// - HirKind::Literal contains a String instead of a Box<[u8]>, because we do not support byte patterns.
/// - HirKind::Class omits the ClassBytes variant, because we do not support byte patterns.
/// - HirKind::Look is not present. We do not support look for now.
/// - HirKind::Capture is not present. We do not need to support capture group because
///   we only use regex for filtering. It should be flatten to its inner pattern.
#[derive(Clone, Debug)]
pub enum ChromaHir {
    Empty,
    Literal(String),
    Class(ClassUnicode),
    Repetition {
        min: u32,
        max: Option<u32>,
        sub: Box<ChromaHir>,
    },
    Concat(Vec<ChromaHir>),
    Alternation(Vec<ChromaHir>),
}

impl TryFrom<hir::Hir> for ChromaHir {
    type Error = ChromaRegexError;

    fn try_from(value: hir::Hir) -> Result<Self, Self::Error> {
        match value.into_kind() {
            HirKind::Empty | HirKind::Look(_) => Ok(Self::Empty),
            HirKind::Literal(literal) => String::from_utf8(literal.0.into_vec())
                .map(Self::Literal)
                .map_err(|_| ChromaRegexError::BytePattern),
            HirKind::Class(class) => match class {
                Class::Unicode(class_unicode) => Ok(Self::Class(class_unicode)),
                Class::Bytes(_) => Err(ChromaRegexError::BytePattern),
            },
            HirKind::Repetition(repetition) => {
                let inner = (*repetition.sub).try_into()?;
                if let Self::Empty = inner {
                    Ok(Self::Empty)
                } else {
                    Ok(Self::Repetition {
                        min: repetition.min,
                        max: repetition.max,
                        sub: Box::new(inner),
                    })
                }
            }
            HirKind::Capture(capture) => (*capture.sub).try_into(),
            HirKind::Concat(hirs) => {
                let mut chroma_hirs = Vec::new();
                for hir in hirs {
                    let chroma_hir = hir.try_into()?;
                    if !matches!(chroma_hir, Self::Empty) {
                        chroma_hirs.push(chroma_hir);
                    }
                }
                if chroma_hirs.len() > 1 {
                    Ok(Self::Concat(chroma_hirs))
                } else if let Some(hir) = chroma_hirs.pop() {
                    Ok(hir)
                } else {
                    Ok(Self::Empty)
                }
            }
            HirKind::Alternation(hirs) => hirs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map(Self::Alternation),
        }
    }
}

impl From<ChromaHir> for hir::Hir {
    fn from(value: ChromaHir) -> Self {
        match value {
            ChromaHir::Empty => Self::empty(),
            ChromaHir::Literal(literal) => Self::literal(literal.into_bytes()),
            ChromaHir::Class(class_unicode) => Self::class(Class::Unicode(class_unicode)),
            ChromaHir::Repetition { min, max, sub } => Self::repetition(Repetition {
                min,
                max,
                greedy: false,
                sub: Box::new((*sub).into()),
            }),
            ChromaHir::Concat(chroma_hirs) => {
                Self::concat(chroma_hirs.into_iter().map(Into::into).collect())
            }
            ChromaHir::Alternation(chroma_hirs) => {
                Self::alternation(chroma_hirs.into_iter().map(Into::into).collect())
            }
        }
    }
}

impl From<ChromaHir> for String {
    fn from(value: ChromaHir) -> Self {
        format!("{}", hir::Hir::from(value))
    }
}
