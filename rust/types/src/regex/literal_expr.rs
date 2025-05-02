use regex_syntax::hir::ClassUnicode;

use super::hir::Hir;

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

impl From<Hir> for LiteralExpr {
    fn from(value: Hir) -> Self {
        match value {
            Hir::Empty => Self::Literal(Vec::new()),
            Hir::Literal(literal) => Self::Literal(literal.chars().map(Literal::Char).collect()),
            Hir::Class(class_unicode) => Self::Literal(vec![Literal::Class(class_unicode)]),
            Hir::Repetition { min, max: _, sub } => {
                let mut repeat = vec![*sub; min as usize];
                // Append a breakpoint Hir to prevent merge with literal on the right
                repeat.push(Hir::Alternation(vec![Hir::Empty]));
                Hir::Concat(repeat).into()
            }
            Hir::Concat(hirs) => {
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
            Hir::Alternation(hirs) => Self::Alternation(hirs.into_iter().map(Into::into).collect()),
        }
    }
}
