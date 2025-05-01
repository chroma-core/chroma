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
            Hir::Repetition { min, max: _, sub } => Hir::Concat(vec![*sub; min as usize]).into(),
            Hir::Concat(hirs) => {
                let exprs = hirs
                    .into_iter()
                    .flat_map(|hir| match LiteralExpr::from(hir) {
                        LiteralExpr::Concat(literal_exprs) => literal_exprs.into_iter(),
                        expr => vec![expr].into_iter(),
                    })
                    .fold(Vec::new(), |mut exprs, expr| {
                        match (exprs.last_mut(), expr) {
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
