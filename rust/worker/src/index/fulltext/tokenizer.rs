use crate::errors::{ChromaError, ErrorCodes};

use tantivy::tokenizer::{NgramTokenizer, Token, Tokenizer, TokenStream};

pub(crate) trait ChromaTokenStream {
    fn process(&mut self, sink: &mut dyn FnMut(&Token));
    fn get_tokens(&self) -> &Vec<Token>;
}

pub(crate) struct TantivyChromaTokenStream {
    tokens: Vec<Token>
}

impl TantivyChromaTokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        TantivyChromaTokenStream {
            tokens,
        }
    }
}

impl ChromaTokenStream for TantivyChromaTokenStream {
    fn process(&mut self, sink: &mut dyn FnMut(&Token)) {
        for token in &self.tokens {
            sink(token);
        }
    }

    fn get_tokens(&self) -> &Vec<Token> {
        &self.tokens
    }
}

pub(crate) trait ChromaTokenizer {
    fn encode(&mut self, text: &str) -> Box<dyn ChromaTokenStream>;
}

pub(crate) struct TantivyChromaTokenizer {
    tokenizer: Box<NgramTokenizer>
}

impl TantivyChromaTokenizer {
    pub fn new(tokenizer: Box<NgramTokenizer>) -> Self {
        TantivyChromaTokenizer {
            tokenizer,
        }
    }
}

impl ChromaTokenizer for TantivyChromaTokenizer {
    fn encode(&mut self, text: &str) -> Box<dyn ChromaTokenStream> {
        let mut token_stream = self.tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            tokens.push(token.clone());
        });
        Box::new(TantivyChromaTokenStream::new(tokens))
    }
}

mod test {
    use super::*;

    #[test]
    fn test_chroma_tokenizer() {
        let tokenizer: Box<NgramTokenizer> = Box::new(NgramTokenizer::new(1, 1, false).unwrap());
        let mut chroma_tokenizer = TantivyChromaTokenizer::new(tokenizer);
        let mut token_stream = chroma_tokenizer.encode("hello world");
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            tokens.push(token.clone());
        });
        assert_eq!(tokens.len(), 11);
        assert_eq!(tokens[0].text, "h");
        assert_eq!(tokens[1].text, "e");
    }

    #[test]
    fn test_get_tokens() {
        let tokenizer: Box<NgramTokenizer> = Box::new(NgramTokenizer::new(1, 1, false).unwrap());
        let mut chroma_tokenizer = TantivyChromaTokenizer::new(tokenizer);
        let token_stream = chroma_tokenizer.encode("hello world");
        let tokens = token_stream.get_tokens();
        assert_eq!(tokens.len(), 11);
        assert_eq!(tokens[0].text, "h");
        assert_eq!(tokens[1].text, "e");
    }
}