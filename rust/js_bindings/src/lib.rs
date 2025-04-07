#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use chroma_cli::chroma_cli;

#[napi]
pub fn cli(args: Option<Vec<String>>) -> napi::Result<()> {
  let args = args.unwrap_or_else(|| std::env::args().collect());
  let args = if args.is_empty() {
    vec!["chroma".to_string()]
  } else {
    args
  };
  chroma_cli(args);
  Ok(())
}
