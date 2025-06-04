use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};

pub fn generate_compile_error(error_message: &str) -> TokenStream {
    let mut token_stream = TokenStream::new();
    token_stream.extend([TokenTree::Ident(Ident::new(
        "compile_error",
        Span::call_site(),
    ))]);

    token_stream.extend([TokenTree::Punct(Punct::new('!', Spacing::Alone))]);

    let literal = Literal::string(error_message);
    let mut inner = TokenStream::new();
    inner.extend([TokenTree::Literal(literal)]);
    let group = Group::new(Delimiter::Parenthesis, inner);
    token_stream.extend([TokenTree::Group(group)]);

    token_stream.extend([TokenTree::Punct(Punct::new(';', Spacing::Alone))]);

    token_stream
}