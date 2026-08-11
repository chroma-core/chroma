use std::error::Error;

use chroma_types::chroma_proto::fault_action::Act;
use chroma_types::chroma_proto::fault_injection_service_client::FaultInjectionServiceClient;
use chroma_types::chroma_proto::fault_selector::By;
use chroma_types::chroma_proto::{
    ActionDelay, ActionUnavailable, ClearFaultsRequest, FaultAction, FaultEntry, FaultSelector,
    InjectFaultsRequest, ListFaultsRequest, SelectFileLine, SelectLabel,
};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use tonic::transport::Channel;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum TiltInstance {
    Chroma,
    Chroma2,
}

impl TiltInstance {
    fn default_addr(self) -> &'static str {
        match self {
            TiltInstance::Chroma => "http://127.0.0.1:50054",
            TiltInstance::Chroma2 => "http://127.0.0.1:60054",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "chroma-fault",
    about = "Inject, inspect, and clear faults against Tilt's rust-log-service."
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Fault injection service address. Defaults to the selected Tilt instance."
    )]
    addr: Option<String>,

    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = TiltInstance::Chroma,
        help = "Tilt instance to target when --addr is omitted."
    )]
    tilt_instance: TiltInstance,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inject a new fault
    Inject(InjectArgs),
    /// List all injected faults
    List,
    /// Clear one fault selector or all faults
    Clear(ClearArgs),
}

#[derive(Debug, Args)]
struct InjectArgs {
    #[command(flatten)]
    selector: SelectorArgs,

    #[command(flatten)]
    action: ActionArgs,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("selector")
        .required(true)
        .multiple(false)
        .args(["label", "file"])
))]
struct SelectorArgs {
    #[arg(long, help = "Match a named fault label.")]
    label: Option<String>,

    #[arg(long, requires = "line", help = "Match a specific source file.")]
    file: Option<String>,

    #[arg(long, requires = "file", help = "Match a specific source line.")]
    line: Option<u32>,
}

impl SelectorArgs {
    fn to_proto(&self) -> FaultSelector {
        match (&self.label, &self.file, self.line) {
            (Some(label), None, None) => FaultSelector {
                by: Some(By::Label(SelectLabel {
                    label: label.clone(),
                })),
            },
            (None, Some(file), Some(line)) => FaultSelector {
                by: Some(By::FileLine(SelectFileLine {
                    file: file.clone(),
                    line,
                })),
            },
            _ => unreachable!("clap guarantees selector validation"),
        }
    }
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("action")
        .required(true)
        .multiple(false)
        .args(["unavailable", "delay_seconds"])
))]
struct ActionArgs {
    #[arg(long, help = "Return UNAVAILABLE for matching requests.")]
    unavailable: bool,

    #[arg(long, help = "Delay matching requests by this many seconds.")]
    delay_seconds: Option<u64>,
}

impl ActionArgs {
    fn to_proto(&self) -> FaultAction {
        let act = if self.unavailable {
            Act::Unavailable(ActionUnavailable {})
        } else if let Some(delay_seconds) = self.delay_seconds {
            Act::Delay(ActionDelay { delay_seconds })
        } else {
            unreachable!("clap guarantees action validation")
        };
        FaultAction { act: Some(act) }
    }
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("clear_target")
        .required(true)
        .multiple(false)
        .args(["all", "label", "file"])
))]
struct ClearArgs {
    #[arg(long, help = "Clear every injected fault.")]
    all: bool,

    #[arg(long, help = "Clear faults matching this label.")]
    label: Option<String>,

    #[arg(
        long,
        requires = "line",
        help = "Clear faults matching this source file."
    )]
    file: Option<String>,

    #[arg(
        long,
        requires = "file",
        help = "Clear faults matching this source line."
    )]
    line: Option<u32>,
}

impl ClearArgs {
    fn selector(&self) -> Option<FaultSelector> {
        if self.all {
            return None;
        }

        Some(match (&self.label, &self.file, self.line) {
            (Some(label), None, None) => FaultSelector {
                by: Some(By::Label(SelectLabel {
                    label: label.clone(),
                })),
            },
            (None, Some(file), Some(line)) => FaultSelector {
                by: Some(By::FileLine(SelectFileLine {
                    file: file.clone(),
                    line,
                })),
            },
            _ => unreachable!("clap guarantees clear target validation"),
        })
    }
}

fn resolved_addr(cli: &Cli) -> String {
    cli.addr
        .clone()
        .unwrap_or_else(|| cli.tilt_instance.default_addr().to_string())
}

fn format_selector(selector: &FaultSelector) -> String {
    match selector.by.as_ref() {
        Some(By::Label(SelectLabel { label })) => format!("label({label})"),
        Some(By::FileLine(SelectFileLine { file, line })) => format!("file({file}:{line})"),
        None => "<invalid selector>".to_string(),
    }
}

fn format_action(action: &FaultAction) -> String {
    match action.act.as_ref() {
        Some(Act::Unavailable(_)) => "unavailable".to_string(),
        Some(Act::Delay(ActionDelay { delay_seconds })) => format!("delay({delay_seconds}s)"),
        None => "<invalid action>".to_string(),
    }
}

fn format_entry(entry: &FaultEntry) -> String {
    let selector = entry
        .selector
        .as_ref()
        .map(format_selector)
        .unwrap_or_else(|| "<missing selector>".to_string());
    let action = entry
        .action
        .as_ref()
        .map(format_action)
        .unwrap_or_else(|| "<missing action>".to_string());
    format!("{selector} -> {action}")
}

async fn connect(addr: &str) -> Result<FaultInjectionServiceClient<Channel>, Box<dyn Error>> {
    let channel = Channel::from_shared(addr.to_string())?.connect().await?;
    Ok(FaultInjectionServiceClient::new(channel))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let addr = resolved_addr(&cli);
    let mut client = connect(&addr).await?;

    match cli.command {
        Command::Inject(args) => {
            let selector = args.selector.to_proto();
            let action = args.action.to_proto();
            client
                .inject_faults(InjectFaultsRequest {
                    selector: Some(selector.clone()),
                    action: Some(action),
                })
                .await?;
            println!(
                "Injected {} on {}",
                format_action(&action),
                format_selector(&selector)
            );
        }
        Command::List => {
            let response = client.list_faults(ListFaultsRequest {}).await?.into_inner();
            if response.faults.is_empty() {
                println!("No faults configured.");
            } else {
                for (idx, fault) in response.faults.iter().enumerate() {
                    println!("{}. {}", idx + 1, format_entry(fault));
                }
            }
        }
        Command::Clear(args) => {
            let response = client
                .clear_faults(ClearFaultsRequest {
                    id: None,
                    selector: args.selector(),
                })
                .await?
                .into_inner();
            println!("Cleared {} faults.", response.cleared_count);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tilt_default_addresses_match_tiltfile_port_forwards() {
        assert_eq!(
            TiltInstance::Chroma.default_addr(),
            "http://127.0.0.1:50054"
        );
        assert_eq!(
            TiltInstance::Chroma2.default_addr(),
            "http://127.0.0.1:60054"
        );
    }

    #[test]
    fn explicit_address_overrides_tilt_instance_default() {
        let cli = Cli {
            addr: Some("http://localhost:7000".to_string()),
            tilt_instance: TiltInstance::Chroma2,
            command: Command::List,
        };

        assert_eq!(resolved_addr(&cli), "http://localhost:7000");
    }

    #[test]
    fn selector_args_build_label_selector() {
        let selector = SelectorArgs {
            label: Some("fragment-upload".to_string()),
            file: None,
            line: None,
        }
        .to_proto();

        assert_eq!(
            selector.by,
            Some(By::Label(SelectLabel {
                label: "fragment-upload".to_string(),
            }))
        );
    }

    #[test]
    fn action_args_build_delay_action() {
        let action = ActionArgs {
            unavailable: false,
            delay_seconds: Some(7),
        }
        .to_proto();

        assert_eq!(
            action.act,
            Some(Act::Delay(ActionDelay { delay_seconds: 7 }))
        );
    }

    #[test]
    fn clear_all_omits_selector() {
        let clear = ClearArgs {
            all: true,
            label: None,
            file: None,
            line: None,
        };

        assert_eq!(clear.selector(), None);
    }
}
