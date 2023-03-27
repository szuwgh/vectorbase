pub struct Token {
    pub text: String,
}

trait Tokenizer {}

pub trait TokenStream {
    fn next() {}
}

pub struct SimpleToken {}
