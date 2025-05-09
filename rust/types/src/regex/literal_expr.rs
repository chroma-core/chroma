use regex_syntax::hir::ClassUnicode;

use super::hir::ChromaHir;

#[derive(Clone, Debug)]
pub enum Literal {
    Char(char),
    Class(ClassUnicode),
}

#[derive(Clone, Debug)]
pub enum LiteralExpr {
    Literal(Vec<Literal>),
    Concat(Vec<LiteralExpr>),
    Alternation(Vec<LiteralExpr>),
}

impl From<ChromaHir> for LiteralExpr {
    fn from(value: ChromaHir) -> Self {
        match value {
            ChromaHir::Empty => Self::Literal(Vec::new()),
            ChromaHir::Literal(literal) => {
                Self::Literal(literal.chars().map(Literal::Char).collect())
            }
            ChromaHir::Class(class_unicode) => Self::Literal(vec![Literal::Class(class_unicode)]),
            ChromaHir::Repetition { min, max: _, sub } => {
                let mut repeat = vec![*sub; min as usize];
                // Append a breakpoint Hir to prevent merge with literal on the right
                repeat.push(ChromaHir::Alternation(vec![ChromaHir::Empty]));
                ChromaHir::Concat(repeat).into()
            }
            ChromaHir::Concat(hirs) => {
                let exprs = hirs.into_iter().fold(Vec::new(), |mut exprs, expr| {
                    match (exprs.last_mut(), expr.into()) {
                        (Some(Self::Literal(literal)), Self::Literal(extra_literal)) => {
                            literal.extend(extra_literal)
                        }
                        (_, expr) => exprs.push(expr),
                    }
                    exprs
                });
                Self::Concat(exprs)
            }
            ChromaHir::Alternation(hirs) => {
                Self::Alternation(hirs.into_iter().map(Into::into).collect())
            }
        }
    }
}
