use chroma_cli::{run_command_from_args, Cli};
use clap::Parser;
use neon::prelude::*;

fn run_cli(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let args: Vec<Handle<JsValue>> = cx.argument::<JsArray>(0)?.to_vec(&mut cx)?;

    let args: Vec<String> = args
        .into_iter()
        .map(|arg| {
            arg.downcast::<JsString, FunctionContext>(&mut cx)
                .unwrap()
                .value(&mut cx)
        })
        .collect();

    let parsed_args = Cli::parse_from(args);
    run_command_from_args(parsed_args);

    Ok(cx.undefined())
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("run_cli", run_cli)?;
    Ok(())
}
