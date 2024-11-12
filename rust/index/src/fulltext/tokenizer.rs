use tantivy::tokenizer::{NgramTokenizer, Token, TokenStream, Tokenizer};

pub trait ChromaTokenStream: Send {
    fn get_tokens(&self) -> &Vec<Token>;
}

pub struct TantivyChromaTokenStream {
    tokens: Vec<Token>,
}

impl TantivyChromaTokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        TantivyChromaTokenStream { tokens }
    }
}

impl ChromaTokenStream for TantivyChromaTokenStream {
    fn get_tokens(&self) -> &Vec<Token> {
        &self.tokens
    }
}

pub trait ChromaTokenizer: Send + Sync {
    fn encode(&self, text: &str) -> Box<dyn ChromaTokenStream>;
}

pub struct TantivyChromaTokenizer {
    tokenizer: NgramTokenizer,
}

impl TantivyChromaTokenizer {
    pub fn new(tokenizer: NgramTokenizer) -> Self {
        TantivyChromaTokenizer { tokenizer }
    }
}

impl ChromaTokenizer for TantivyChromaTokenizer {
    fn encode(&self, text: &str) -> Box<dyn ChromaTokenStream> {
        let mut tokenizer = self.tokenizer.clone();
        let mut token_stream = tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            tokens.push(token.clone());
        });
        Box::new(TantivyChromaTokenStream::new(tokens))
    }
}

#[cfg(test)]
mod test {
    use super::{ChromaTokenizer, NgramTokenizer, TantivyChromaTokenizer};

    #[test]
    fn test_get_tokens() {
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let chroma_tokenizer = TantivyChromaTokenizer::new(tokenizer);
        let token_stream = chroma_tokenizer.encode("hello world");
        let tokens = token_stream.get_tokens();
        assert_eq!(tokens.len(), 11);
        assert_eq!(tokens[0].text, "h");
        assert_eq!(tokens[1].text, "e");
    }
}
