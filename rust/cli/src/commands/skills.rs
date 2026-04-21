use crate::style;
use crate::terminal::{SystemTerminal, Terminal};
use crate::ui::{
    print_command_hint, print_section_header, print_status_line, print_success_banner,
    print_summary_panel, FilterableMultiSelectPrompt, FilterableSelectItem, PanelSelectPrompt,
};
use crate::utils::{CliError, UtilsError};
use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;
use reqwest::header::USER_AGENT;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

const SKILLS_REGISTRY_URL: &str =
    "https://raw.githubusercontent.com/chroma-core/agent-skills/main/skills/registry.json";
const SKILLS_RAW_BASE_URL: &str =
    "https://raw.githubusercontent.com/chroma-core/agent-skills/main/skills/";
const DEFAULT_SKILL_NAME: &str = "skill";

#[derive(Debug, Error)]
pub enum SkillsError {
    #[error("Failed to get runtime for skills commands")]
    RuntimeError,
    #[error("Failed to fetch the skills registry")]
    RegistryFetchFailed,
    #[error("Failed to parse the skills registry")]
    RegistryParseFailed,
    #[error("No such skill {0}")]
    NoSuchSkill(String),
    #[error("The skills registry entry for {0} is invalid")]
    InvalidRegistryEntry(String),
    #[error("Failed to download skill {0}")]
    SkillDownloadFailed(String),
    #[error("Downloaded skill {0} did not contain any files")]
    EmptySkill(String),
    #[error("No supported agents were detected on this machine")]
    NoDetectedAgents,
    #[error("No agents were selected for installation")]
    NoAgentsSelected,
    #[error("No such agent {0}")]
    NoSuchAgent(String),
    #[error("Global installation is not supported for agent {0}")]
    GlobalInstallUnsupported(String),
    #[error("Failed to read the current working directory")]
    CurrentDirUnavailable,
    #[error("Computed install path escaped its base directory")]
    UnsafeInstallPath,
    #[error("Failed to create skills directory")]
    CreateDirFailed,
    #[error("Failed to remove existing skill installation")]
    RemoveExistingFailed,
    #[error("Failed to write skill files")]
    WriteSkillFailed,
    #[error("Failed to create skill symlink")]
    SymlinkFailed,
}

#[derive(Parser, Debug)]
pub struct SkillsArgs {
    #[command(subcommand)]
    command: Option<SkillsSubcommand>,
}

#[derive(Subcommand, Debug)]
enum SkillsSubcommand {
    #[command(about = "List available Chroma skills")]
    List,
    #[command(about = "Install a Chroma skill")]
    Install(InstallSkillArgs),
}

#[derive(Parser, Debug, Default)]
pub struct InstallSkillArgs {
    #[clap(index = 1, help = "The name of the skill to install")]
    name: Option<String>,
    #[clap(
        long,
        value_delimiter = ',',
        action = clap::ArgAction::Append,
        help = "Agent ids to install into"
    )]
    agent: Vec<String>,
    #[clap(
        long,
        default_value_t = false,
        help = "Install into all detected agents"
    )]
    all_detected: bool,
    #[clap(long, value_enum, help = "Install scope")]
    scope: Option<InstallScope>,
    #[clap(long, value_enum, help = "Install method")]
    mode: Option<InstallMode>,
}

impl InstallSkillArgs {
    pub(crate) fn for_skill(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum InstallScope {
    Project,
    Global,
}

impl InstallScope {
    fn label(&self) -> &'static str {
        match self {
            InstallScope::Project => "project",
            InstallScope::Global => "global",
        }
    }

    fn summary_label(&self) -> &'static str {
        match self {
            InstallScope::Project => "Project",
            InstallScope::Global => "Global",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum InstallMode {
    Copy,
    Symlink,
}

impl InstallMode {
    fn label(&self) -> &'static str {
        match self {
            InstallMode::Copy => "copy",
            InstallMode::Symlink => "symlink",
        }
    }

    fn summary_label(&self) -> &'static str {
        match self {
            InstallMode::Copy => "Copy (direct per-agent copy)",
            InstallMode::Symlink => "Symlink (canonical shared install)",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AgentDefinition {
    id: &'static str,
    display_name: &'static str,
    universal: bool,
    project_dir: &'static str,
    global_dir: GlobalDirKind,
    detection: DetectionKind,
    selectable: bool,
}

#[derive(Debug, Clone, Copy)]
enum GlobalDirKind {
    HomeDotSkills(&'static str),
    HomeAgentsSkills,
    XdgAgentsSkills,
    Antigravity,
    Claude,
    Openclaw,
    Codex,
    Cortex,
    Crush,
    Deepagents,
    Gemini,
    Goose,
    Kimi,
    Opencode,
    Pi,
    Windsurf,
}

#[derive(Debug, Clone, Copy)]
enum DetectionKind {
    HomeDot(&'static str),
    CwdOrHomeDot(&'static str),
    Amp,
    Antigravity,
    Claude,
    Openclaw,
    Codex,
    Cortex,
    Crush,
    Goose,
    Opencode,
    Pi,
    Replit,
    Windsurf,
}

#[derive(Debug, Clone)]
struct InstallContext {
    cwd: PathBuf,
    home: PathBuf,
    xdg_config: PathBuf,
}

#[derive(Debug, Clone)]
struct RemoteSkillFile {
    relative_path: PathBuf,
    contents: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct SkillsRegistry {
    skills: Vec<RegistrySkill>,
}

#[derive(Debug, Deserialize)]
struct RegistrySkill {
    name: String,
    description: String,
    path: String,
    #[serde(default)]
    topics: Vec<RegistryTopic>,
    #[serde(default)]
    general: Vec<RegistryGeneralDoc>,
}

#[derive(Debug, Deserialize)]
struct RegistryTopic {
    paths: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RegistryGeneralDoc {
    path: String,
}

const AGENTS: &[AgentDefinition] = &[
    AgentDefinition {
        id: "amp",
        display_name: "Amp",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::XdgAgentsSkills,
        detection: DetectionKind::Amp,
        selectable: true,
    },
    AgentDefinition {
        id: "antigravity",
        display_name: "Antigravity",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::Antigravity,
        detection: DetectionKind::Antigravity,
        selectable: true,
    },
    AgentDefinition {
        id: "augment",
        display_name: "Augment",
        universal: false,
        project_dir: ".augment/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".augment"),
        detection: DetectionKind::HomeDot(".augment"),
        selectable: true,
    },
    AgentDefinition {
        id: "bob",
        display_name: "IBM Bob",
        universal: false,
        project_dir: ".bob/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".bob"),
        detection: DetectionKind::HomeDot(".bob"),
        selectable: true,
    },
    AgentDefinition {
        id: "claude-code",
        display_name: "Claude Code",
        universal: false,
        project_dir: ".claude/skills",
        global_dir: GlobalDirKind::Claude,
        detection: DetectionKind::Claude,
        selectable: true,
    },
    AgentDefinition {
        id: "openclaw",
        display_name: "OpenClaw",
        universal: false,
        project_dir: "skills",
        global_dir: GlobalDirKind::Openclaw,
        detection: DetectionKind::Openclaw,
        selectable: true,
    },
    AgentDefinition {
        id: "cline",
        display_name: "Cline",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::HomeAgentsSkills,
        detection: DetectionKind::HomeDot(".cline"),
        selectable: true,
    },
    AgentDefinition {
        id: "codebuddy",
        display_name: "CodeBuddy",
        universal: false,
        project_dir: ".codebuddy/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".codebuddy"),
        detection: DetectionKind::CwdOrHomeDot(".codebuddy"),
        selectable: true,
    },
    AgentDefinition {
        id: "codex",
        display_name: "Codex",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::Codex,
        detection: DetectionKind::Codex,
        selectable: true,
    },
    AgentDefinition {
        id: "command-code",
        display_name: "Command Code",
        universal: false,
        project_dir: ".commandcode/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".commandcode"),
        detection: DetectionKind::HomeDot(".commandcode"),
        selectable: true,
    },
    AgentDefinition {
        id: "continue",
        display_name: "Continue",
        universal: false,
        project_dir: ".continue/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".continue"),
        detection: DetectionKind::CwdOrHomeDot(".continue"),
        selectable: true,
    },
    AgentDefinition {
        id: "cortex",
        display_name: "Cortex Code",
        universal: false,
        project_dir: ".cortex/skills",
        global_dir: GlobalDirKind::Cortex,
        detection: DetectionKind::Cortex,
        selectable: true,
    },
    AgentDefinition {
        id: "crush",
        display_name: "Crush",
        universal: false,
        project_dir: ".crush/skills",
        global_dir: GlobalDirKind::Crush,
        detection: DetectionKind::Crush,
        selectable: true,
    },
    AgentDefinition {
        id: "cursor",
        display_name: "Cursor",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".cursor"),
        detection: DetectionKind::HomeDot(".cursor"),
        selectable: true,
    },
    AgentDefinition {
        id: "deepagents",
        display_name: "Deep Agents",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::Deepagents,
        detection: DetectionKind::HomeDot(".deepagents"),
        selectable: true,
    },
    AgentDefinition {
        id: "droid",
        display_name: "Droid",
        universal: false,
        project_dir: ".factory/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".factory"),
        detection: DetectionKind::HomeDot(".factory"),
        selectable: true,
    },
    AgentDefinition {
        id: "firebender",
        display_name: "Firebender",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".firebender"),
        detection: DetectionKind::HomeDot(".firebender"),
        selectable: true,
    },
    AgentDefinition {
        id: "gemini-cli",
        display_name: "Gemini CLI",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::Gemini,
        detection: DetectionKind::HomeDot(".gemini"),
        selectable: true,
    },
    AgentDefinition {
        id: "github-copilot",
        display_name: "GitHub Copilot",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".copilot"),
        detection: DetectionKind::HomeDot(".copilot"),
        selectable: true,
    },
    AgentDefinition {
        id: "goose",
        display_name: "Goose",
        universal: false,
        project_dir: ".goose/skills",
        global_dir: GlobalDirKind::Goose,
        detection: DetectionKind::Goose,
        selectable: true,
    },
    AgentDefinition {
        id: "junie",
        display_name: "Junie",
        universal: false,
        project_dir: ".junie/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".junie"),
        detection: DetectionKind::HomeDot(".junie"),
        selectable: true,
    },
    AgentDefinition {
        id: "iflow-cli",
        display_name: "iFlow CLI",
        universal: false,
        project_dir: ".iflow/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".iflow"),
        detection: DetectionKind::HomeDot(".iflow"),
        selectable: true,
    },
    AgentDefinition {
        id: "kilo",
        display_name: "Kilo Code",
        universal: false,
        project_dir: ".kilocode/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".kilocode"),
        detection: DetectionKind::HomeDot(".kilocode"),
        selectable: true,
    },
    AgentDefinition {
        id: "kimi-cli",
        display_name: "Kimi Code CLI",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::Kimi,
        detection: DetectionKind::HomeDot(".kimi"),
        selectable: true,
    },
    AgentDefinition {
        id: "kiro-cli",
        display_name: "Kiro CLI",
        universal: false,
        project_dir: ".kiro/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".kiro"),
        detection: DetectionKind::HomeDot(".kiro"),
        selectable: true,
    },
    AgentDefinition {
        id: "kode",
        display_name: "Kode",
        universal: false,
        project_dir: ".kode/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".kode"),
        detection: DetectionKind::HomeDot(".kode"),
        selectable: true,
    },
    AgentDefinition {
        id: "mcpjam",
        display_name: "MCPJam",
        universal: false,
        project_dir: ".mcpjam/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".mcpjam"),
        detection: DetectionKind::HomeDot(".mcpjam"),
        selectable: true,
    },
    AgentDefinition {
        id: "mistral-vibe",
        display_name: "Mistral Vibe",
        universal: false,
        project_dir: ".vibe/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".vibe"),
        detection: DetectionKind::HomeDot(".vibe"),
        selectable: true,
    },
    AgentDefinition {
        id: "mux",
        display_name: "Mux",
        universal: false,
        project_dir: ".mux/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".mux"),
        detection: DetectionKind::HomeDot(".mux"),
        selectable: true,
    },
    AgentDefinition {
        id: "opencode",
        display_name: "OpenCode",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::Opencode,
        detection: DetectionKind::Opencode,
        selectable: true,
    },
    AgentDefinition {
        id: "openhands",
        display_name: "OpenHands",
        universal: false,
        project_dir: ".openhands/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".openhands"),
        detection: DetectionKind::HomeDot(".openhands"),
        selectable: true,
    },
    AgentDefinition {
        id: "pi",
        display_name: "Pi",
        universal: false,
        project_dir: ".pi/skills",
        global_dir: GlobalDirKind::Pi,
        detection: DetectionKind::Pi,
        selectable: true,
    },
    AgentDefinition {
        id: "qoder",
        display_name: "Qoder",
        universal: false,
        project_dir: ".qoder/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".qoder"),
        detection: DetectionKind::HomeDot(".qoder"),
        selectable: true,
    },
    AgentDefinition {
        id: "qwen-code",
        display_name: "Qwen Code",
        universal: false,
        project_dir: ".qwen/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".qwen"),
        detection: DetectionKind::HomeDot(".qwen"),
        selectable: true,
    },
    AgentDefinition {
        id: "replit",
        display_name: "Replit",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::XdgAgentsSkills,
        detection: DetectionKind::Replit,
        selectable: false,
    },
    AgentDefinition {
        id: "roo",
        display_name: "Roo Code",
        universal: false,
        project_dir: ".roo/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".roo"),
        detection: DetectionKind::HomeDot(".roo"),
        selectable: true,
    },
    AgentDefinition {
        id: "trae",
        display_name: "Trae",
        universal: false,
        project_dir: ".trae/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".trae"),
        detection: DetectionKind::HomeDot(".trae"),
        selectable: true,
    },
    AgentDefinition {
        id: "trae-cn",
        display_name: "Trae CN",
        universal: false,
        project_dir: ".trae/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".trae-cn"),
        detection: DetectionKind::HomeDot(".trae-cn"),
        selectable: true,
    },
    AgentDefinition {
        id: "warp",
        display_name: "Warp",
        universal: true,
        project_dir: ".agents/skills",
        global_dir: GlobalDirKind::HomeAgentsSkills,
        detection: DetectionKind::HomeDot(".warp"),
        selectable: true,
    },
    AgentDefinition {
        id: "windsurf",
        display_name: "Windsurf",
        universal: false,
        project_dir: ".windsurf/skills",
        global_dir: GlobalDirKind::Windsurf,
        detection: DetectionKind::Windsurf,
        selectable: true,
    },
    AgentDefinition {
        id: "zencoder",
        display_name: "Zencoder",
        universal: false,
        project_dir: ".zencoder/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".zencoder"),
        detection: DetectionKind::HomeDot(".zencoder"),
        selectable: true,
    },
    AgentDefinition {
        id: "neovate",
        display_name: "Neovate",
        universal: false,
        project_dir: ".neovate/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".neovate"),
        detection: DetectionKind::HomeDot(".neovate"),
        selectable: true,
    },
    AgentDefinition {
        id: "pochi",
        display_name: "Pochi",
        universal: false,
        project_dir: ".pochi/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".pochi"),
        detection: DetectionKind::HomeDot(".pochi"),
        selectable: true,
    },
    AgentDefinition {
        id: "adal",
        display_name: "AdaL",
        universal: false,
        project_dir: ".adal/skills",
        global_dir: GlobalDirKind::HomeDotSkills(".adal"),
        detection: DetectionKind::HomeDot(".adal"),
        selectable: true,
    },
];

pub fn skills(args: SkillsArgs) -> Result<(), CliError> {
    let mut term = SystemTerminal;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| SkillsError::RuntimeError)?;
    runtime.block_on(async {
        match args.command {
            None | Some(SkillsSubcommand::List) => list_skills(&mut term).await,
            Some(SkillsSubcommand::Install(install_args)) => {
                install_skill(install_args, &mut term).await
            }
        }
    })
}

async fn list_skills(term: &mut dyn Terminal) -> Result<(), CliError> {
    let registry = fetch_skills_registry().await?;
    print_section_header(term, "Available Chroma skills:");
    for skill in registry.skills {
        term.println(&format!(
            "{} {} - {}",
            style::list_marker(),
            skill.name.bold(),
            skill.description
        ));
    }
    term.println("");
    print_command_hint(
        term,
        "Install a skill with:",
        "chroma skills install <skill-name>",
    );
    Ok(())
}

pub(crate) async fn install_skill(
    args: InstallSkillArgs,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let context = InstallContext::current()?;
    let registry = fetch_skills_registry().await?;
    let skill = resolve_skill(&registry, args.name, term)?;
    let scope = resolve_scope(args.scope, term)?;
    let mode = resolve_mode(args.mode, term)?;
    let agents = resolve_agents(args.agent, args.all_detected, scope, &context, term)?;

    print_status_line(
        term,
        "Installing",
        &format!(
            "{} into {} [{} / {}]",
            skill.name.bold(),
            summarize_agent_ids(&agents).bold(),
            scope.label(),
            mode.label()
        ),
    );

    let files = download_skill_files(skill).await?;
    install_skill_files(skill, &files, &agents, scope, mode, &context)?;

    print_success_banner(term, &format!("Installed {} successfully.", skill.name));

    Ok(())
}

fn resolve_skill<'a>(
    registry: &'a SkillsRegistry,
    requested_name: Option<String>,
    term: &mut dyn Terminal,
) -> Result<&'a RegistrySkill, CliError> {
    let name = match requested_name {
        Some(name) => name,
        None => prompt_for_skill(registry, term)?,
    };

    let skill = registry
        .skills
        .iter()
        .find(|skill| skill.name == name)
        .ok_or_else(|| SkillsError::NoSuchSkill(name.clone()))?;
    print_summary_panel(term, "Skill", &skill.name);
    Ok(skill)
}

fn prompt_for_skill(
    registry: &SkillsRegistry,
    term: &mut dyn Terminal,
) -> Result<String, CliError> {
    let items = registry
        .skills
        .iter()
        .map(|skill| FilterableSelectItem {
            label: format!("{} - {}", skill.name, skill.description),
            summary: skill.name.clone(),
        })
        .collect::<Vec<_>>();
    let context_lines = vec![
        format!("• Source: {}", SKILLS_REGISTRY_URL),
        format!("• Found {} skills", registry.skills.len()),
    ];
    let selection = term.prompt_panel_select(&PanelSelectPrompt {
        tag: "skills",
        title: "Choose a skill to install",
        context_lines: &context_lines,
        items: &items,
        default_selected_index: 0,
        empty_message: "No skills available.",
    })?;
    Ok(registry.skills[selection].name.clone())
}

fn resolve_scope(
    scope: Option<InstallScope>,
    term: &mut dyn Terminal,
) -> Result<InstallScope, CliError> {
    let scope = if let Some(scope) = scope {
        scope
    } else {
        let options = vec![
            FilterableSelectItem {
                label: "Project - install into this repository".to_string(),
                summary: "Project".to_string(),
            },
            FilterableSelectItem {
                label: "Global - install for agents on this machine".to_string(),
                summary: "Global".to_string(),
            },
        ];
        let selection = term.prompt_panel_select(&PanelSelectPrompt {
            tag: "skills",
            title: "Choose install scope",
            context_lines: &[],
            items: &options,
            default_selected_index: 0,
            empty_message: "No install scopes available.",
        })?;
        match selection {
            0 => InstallScope::Project,
            _ => InstallScope::Global,
        }
    };
    print_summary_panel(term, "Installation scope", scope.summary_label());
    Ok(scope)
}

fn resolve_mode(
    mode: Option<InstallMode>,
    term: &mut dyn Terminal,
) -> Result<InstallMode, CliError> {
    let mode = if let Some(mode) = mode {
        mode
    } else {
        let options = vec![
            FilterableSelectItem {
                label: "Symlink - canonical shared install".to_string(),
                summary: "Symlink".to_string(),
            },
            FilterableSelectItem {
                label: "Copy - direct per-agent copy".to_string(),
                summary: "Copy".to_string(),
            },
        ];
        let selection = term.prompt_panel_select(&PanelSelectPrompt {
            tag: "skills",
            title: "Choose install method",
            context_lines: &[],
            items: &options,
            default_selected_index: 0,
            empty_message: "No install methods available.",
        })?;
        match selection {
            0 => InstallMode::Symlink,
            _ => InstallMode::Copy,
        }
    };
    print_summary_panel(term, "Installation method", mode.summary_label());
    Ok(mode)
}

fn resolve_agents(
    requested_agents: Vec<String>,
    all_detected: bool,
    scope: InstallScope,
    context: &InstallContext,
    term: &mut dyn Terminal,
) -> Result<Vec<&'static AgentDefinition>, CliError> {
    let agents = if !requested_agents.is_empty() {
        resolve_agent_ids(requested_agents)?
    } else {
        let detected = detected_agents(context);
        if all_detected {
            if detected.is_empty() {
                return Err(SkillsError::NoDetectedAgents.into());
            }
            detected
        } else {
            match scope {
                InstallScope::Project => prompt_for_project_agents(term)?,
                InstallScope::Global => {
                    if !detected.is_empty() {
                        let detected_names = summarize_agent_names(&detected);
                        print_summary_panel(term, "Detected agents", &detected_names);
                        if prompt_detected_agents_shortcut(term, &detected_names)? {
                            detected
                        } else {
                            prompt_for_global_agents(context, term)?
                        }
                    } else {
                        prompt_for_global_agents(context, term)?
                    }
                }
            }
        }
    };
    print_summary_panel(term, "Agents", &summarize_selected_agents(&agents, scope));
    Ok(agents)
}

fn detected_agents(context: &InstallContext) -> Vec<&'static AgentDefinition> {
    AGENTS
        .iter()
        .filter(|agent| agent.selectable && agent.is_installed(context))
        .collect()
}

fn resolve_agent_ids(ids: Vec<String>) -> Result<Vec<&'static AgentDefinition>, CliError> {
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::new();

    for id in ids {
        if !seen.insert(id.clone()) {
            continue;
        }

        let agent = AGENTS
            .iter()
            .find(|candidate| candidate.id == id && candidate.selectable)
            .ok_or_else(|| SkillsError::NoSuchAgent(id.clone()))?;
        resolved.push(agent);
    }

    if resolved.is_empty() {
        return Err(SkillsError::NoAgentsSelected.into());
    }

    Ok(resolved)
}

fn prompt_detected_agents_shortcut(
    term: &mut dyn Terminal,
    detected_names: &str,
) -> Result<bool, CliError> {
    let context_lines = vec![format!("• Detected agents: {}", detected_names)];
    let options = vec![
        FilterableSelectItem {
            label: "Install to all detected agents".to_string(),
            summary: "Install to detected agents".to_string(),
        },
        FilterableSelectItem {
            label: "Choose agents manually".to_string(),
            summary: "Choose agents manually".to_string(),
        },
    ];

    let selection = term.prompt_panel_select(&PanelSelectPrompt {
        tag: "skills",
        title: "Detected compatible agents",
        context_lines: &context_lines,
        items: &options,
        default_selected_index: 0,
        empty_message: "No choices available.",
    })?;

    Ok(selection == 0)
}

fn prompt_for_project_agents(
    term: &mut dyn Terminal,
) -> Result<Vec<&'static AgentDefinition>, CliError> {
    let universal_agents = AGENTS
        .iter()
        .filter(|agent| agent.selectable && agent.universal)
        .collect::<Vec<_>>();
    let additional_agents = AGENTS
        .iter()
        .filter(|agent| agent.selectable && !agent.universal)
        .collect::<Vec<_>>();
    let total_supported = universal_agents.len() + additional_agents.len();
    let preface_lines = vec![format!("{} supported agents", total_supported)];
    let default_selected_indices = additional_agents
        .iter()
        .enumerate()
        .filter_map(|(index, agent)| (agent.id == "claude-code").then_some(index))
        .collect::<Vec<_>>();

    let included_items = universal_agents
        .iter()
        .map(|agent| FilterableSelectItem {
            label: agent.display_name.to_string(),
            summary: agent.display_name.to_string(),
        })
        .collect::<Vec<_>>();
    let selectable_items = additional_agents
        .iter()
        .map(|agent| FilterableSelectItem {
            label: format!("{} ({})", agent.display_name, agent.project_dir),
            summary: agent.display_name.to_string(),
        })
        .collect::<Vec<_>>();

    let selected = term.prompt_multi_select(&FilterableMultiSelectPrompt {
        tag: "skills",
        title: "Install skill into agents",
        preface_lines: &preface_lines,
        prompt: "Which agents do you want to install to?",
        included_heading: Some("Universal (.agents/skills) - always included"),
        included_items: &included_items,
        selectable_heading: "Additional agents",
        selectable_items: &selectable_items,
        default_selected_indices: &default_selected_indices,
        empty_message: "No matching agents.",
    })?;

    let mut resolved = universal_agents;
    for index in selected {
        if let Some(agent) = additional_agents.get(index) {
            resolved.push(*agent);
        }
    }

    if resolved.is_empty() {
        return Err(SkillsError::NoAgentsSelected.into());
    }

    Ok(resolved)
}

fn prompt_for_global_agents(
    context: &InstallContext,
    term: &mut dyn Terminal,
) -> Result<Vec<&'static AgentDefinition>, CliError> {
    let selectable_agents = AGENTS
        .iter()
        .filter(|agent| agent.selectable)
        .collect::<Vec<_>>();
    let preface_lines = vec![format!("{} supported agents", selectable_agents.len())];
    let selectable_items = selectable_agents
        .iter()
        .map(|agent| FilterableSelectItem {
            label: format!(
                "{} ({})",
                agent.display_name,
                agent
                    .global_dir(context)
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "global install unavailable".to_string())
            ),
            summary: agent.display_name.to_string(),
        })
        .collect::<Vec<_>>();

    let selected = term.prompt_multi_select(&FilterableMultiSelectPrompt {
        tag: "skills",
        title: "Install skill into agents",
        preface_lines: &preface_lines,
        prompt: "Which agents do you want to install to?",
        included_heading: None,
        included_items: &[],
        selectable_heading: "Supported agents",
        selectable_items: &selectable_items,
        default_selected_indices: &[],
        empty_message: "No matching agents.",
    })?;

    let mut resolved = Vec::new();
    for index in selected {
        if let Some(agent) = selectable_agents.get(index) {
            resolved.push(*agent);
        }
    }

    if resolved.is_empty() {
        return Err(SkillsError::NoAgentsSelected.into());
    }

    Ok(resolved)
}

fn summarize_agent_ids(agents: &[&AgentDefinition]) -> String {
    const MAX_IDS: usize = 4;

    if agents.is_empty() {
        return "none".to_string();
    }

    let ids = agents.iter().map(|agent| agent.id).collect::<Vec<_>>();
    if ids.len() <= MAX_IDS {
        return ids.join(", ");
    }

    format!(
        "{}, {}, {}, {} +{} more",
        ids[0],
        ids[1],
        ids[2],
        ids[3],
        ids.len() - MAX_IDS
    )
}

fn summarize_agent_names(agents: &[&AgentDefinition]) -> String {
    const MAX_NAMES: usize = 4;

    if agents.is_empty() {
        return "none".to_string();
    }

    let names = agents
        .iter()
        .map(|agent| agent.display_name)
        .collect::<Vec<_>>();
    if names.len() <= MAX_NAMES {
        return names.join(", ");
    }

    format!(
        "{}, {}, {}, {} +{} more",
        names[0],
        names[1],
        names[2],
        names[3],
        names.len() - MAX_NAMES
    )
}

fn summarize_selected_agents(agents: &[&AgentDefinition], scope: InstallScope) -> String {
    match scope {
        InstallScope::Global => format!(
            "{} [{} selected]",
            summarize_agent_names(agents),
            agents.len()
        ),
        InstallScope::Project => {
            let universal = agents
                .iter()
                .copied()
                .filter(|agent| agent.universal)
                .collect::<Vec<_>>();
            let additional = agents
                .iter()
                .copied()
                .filter(|agent| !agent.universal)
                .collect::<Vec<_>>();

            match (universal.is_empty(), additional.is_empty()) {
                (_, true) => format!(
                    "{} [{} selected]",
                    summarize_agent_names(&universal),
                    agents.len()
                ),
                (true, false) => format!(
                    "{} [{} selected]",
                    summarize_agent_names(&additional),
                    agents.len()
                ),
                (false, false) => format!(
                    "{} [{} selected; additional: {}]",
                    summarize_agent_names(&universal),
                    agents.len(),
                    summarize_agent_names(&additional)
                ),
            }
        }
    }
}

async fn download_skill_files(skill: &RegistrySkill) -> Result<Vec<RemoteSkillFile>, CliError> {
    let client = Client::new();
    let mut files = Vec::new();

    for remote_path in registry_file_paths(skill)? {
        let url = format!("{SKILLS_RAW_BASE_URL}{remote_path}");
        let bytes = client
            .get(url)
            .header(USER_AGENT, "chroma-cli")
            .send()
            .await
            .map_err(|_| SkillsError::SkillDownloadFailed(skill.name.clone()))?
            .error_for_status()
            .map_err(|_| SkillsError::SkillDownloadFailed(skill.name.clone()))?
            .bytes()
            .await
            .map_err(|_| SkillsError::SkillDownloadFailed(skill.name.clone()))?;

        files.push(RemoteSkillFile {
            relative_path: skill_relative_path(skill, &remote_path)?,
            contents: bytes.to_vec(),
        });
    }

    if files.is_empty() {
        return Err(SkillsError::EmptySkill(skill.name.clone()).into());
    }

    Ok(files)
}

async fn fetch_skills_registry() -> Result<SkillsRegistry, CliError> {
    let client = Client::new();
    let response = client
        .get(SKILLS_REGISTRY_URL)
        .header(USER_AGENT, "chroma-cli")
        .send()
        .await
        .map_err(|_| SkillsError::RegistryFetchFailed)?;

    let response = response
        .error_for_status()
        .map_err(|_| SkillsError::RegistryFetchFailed)?;

    response
        .json::<SkillsRegistry>()
        .await
        .map_err(|_| SkillsError::RegistryParseFailed.into())
}

fn registry_file_paths(skill: &RegistrySkill) -> Result<Vec<String>, CliError> {
    let skill_root = skill_root_dir(skill)?;
    let mut seen = BTreeSet::new();
    let mut paths = Vec::new();

    for path in skill_registry_paths(skill) {
        let normalized = normalized_skill_registry_path(skill, &path, &skill_root)?;
        if seen.insert(normalized.clone()) {
            paths.push(normalized);
        }
    }

    Ok(paths)
}

fn skill_registry_paths(skill: &RegistrySkill) -> Vec<String> {
    let mut paths = vec![skill.path.clone()];
    paths.extend(skill.general.iter().map(|entry| entry.path.clone()));
    for topic in &skill.topics {
        paths.extend(topic.paths.values().cloned());
    }
    paths
}

fn skill_root_dir(skill: &RegistrySkill) -> Result<PathBuf, CliError> {
    Path::new(&skill.path)
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| SkillsError::InvalidRegistryEntry(skill.name.clone()).into())
}

fn normalized_skill_registry_path(
    skill: &RegistrySkill,
    remote_path: &str,
    skill_root: &Path,
) -> Result<String, CliError> {
    let path = Path::new(remote_path);
    let relative = path
        .strip_prefix(skill_root)
        .map_err(|_| SkillsError::InvalidRegistryEntry(skill.name.clone()))?;
    safe_join(Path::new("skills"), skill_root)?;
    safe_join(skill_root, relative)?;
    Ok(path.to_string_lossy().into_owned())
}

fn skill_relative_path(skill: &RegistrySkill, remote_path: &str) -> Result<PathBuf, CliError> {
    let skill_root = skill_root_dir(skill)?;
    Path::new(remote_path)
        .strip_prefix(&skill_root)
        .map(Path::to_path_buf)
        .map_err(|_| SkillsError::InvalidRegistryEntry(skill.name.clone()).into())
}

#[derive(Debug)]
struct InstallResult {
    #[cfg_attr(not(test), allow(dead_code))]
    install_kind: String,
}

fn install_skill_files(
    skill: &RegistrySkill,
    files: &[RemoteSkillFile],
    agents: &[&AgentDefinition],
    scope: InstallScope,
    mode: InstallMode,
    context: &InstallContext,
) -> Result<Vec<InstallResult>, CliError> {
    let skill_dir_name = sanitize_skill_name(&skill.name);
    let canonical_base = canonical_base_dir(scope, context);
    let canonical_skill_path = safe_join(&canonical_base, Path::new(&skill_dir_name))?;
    let mut results = Vec::new();

    if mode == InstallMode::Symlink {
        recreate_dir(&canonical_skill_path)?;
        write_skill_files(&canonical_skill_path, files)?;
    }

    for agent in agents {
        let agent_base = agent_base_dir(agent, scope, context)?;
        let agent_skill_path = safe_join(&agent_base, Path::new(&skill_dir_name))?;

        match mode {
            InstallMode::Copy => {
                recreate_dir(&agent_skill_path)?;
                write_skill_files(&agent_skill_path, files)?;
                results.push(InstallResult {
                    install_kind: "copied".to_string(),
                });
            }
            InstallMode::Symlink => {
                if install_path_matches_canonical(&agent_skill_path, &canonical_skill_path) {
                    results.push(InstallResult {
                        install_kind: if agent.universal {
                            "canonical".to_string()
                        } else {
                            "shared".to_string()
                        },
                    });
                    continue;
                }

                match create_symlink_install(&canonical_skill_path, &agent_skill_path) {
                    Ok(()) => results.push(InstallResult {
                        install_kind: "symlinked".to_string(),
                    }),
                    Err(_) => {
                        recreate_dir(&agent_skill_path)?;
                        write_skill_files(&agent_skill_path, files)?;
                        results.push(InstallResult {
                            install_kind: "copied (symlink fallback)".to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(results)
}

fn canonical_base_dir(scope: InstallScope, context: &InstallContext) -> PathBuf {
    match scope {
        InstallScope::Project => context.cwd.join(".agents/skills"),
        InstallScope::Global => context.home.join(".agents/skills"),
    }
}

fn agent_base_dir(
    agent: &AgentDefinition,
    scope: InstallScope,
    context: &InstallContext,
) -> Result<PathBuf, CliError> {
    match scope {
        InstallScope::Project => Ok(context.cwd.join(agent.project_dir)),
        InstallScope::Global => agent.global_dir(context).ok_or_else(|| {
            SkillsError::GlobalInstallUnsupported(agent.display_name.to_string()).into()
        }),
    }
}

fn recreate_dir(path: &Path) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|_| SkillsError::CreateDirFailed)?;
    }
    remove_existing_path(path)?;
    fs::create_dir_all(path).map_err(|_| SkillsError::CreateDirFailed)?;
    Ok(())
}

fn remove_existing_path(path: &Path) -> Result<(), CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() || metadata.is_file() {
                fs::remove_file(path).map_err(|_| SkillsError::RemoveExistingFailed)?;
            } else {
                fs::remove_dir_all(path).map_err(|_| SkillsError::RemoveExistingFailed)?;
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SkillsError::RemoveExistingFailed.into()),
    }
}

fn write_skill_files(target_dir: &Path, files: &[RemoteSkillFile]) -> Result<(), CliError> {
    for file in files {
        let destination = safe_join(target_dir, &file.relative_path)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|_| SkillsError::CreateDirFailed)?;
        }
        fs::write(destination, &file.contents).map_err(|_| SkillsError::WriteSkillFailed)?;
    }
    Ok(())
}

fn install_path_matches_canonical(agent_skill_path: &Path, canonical_skill_path: &Path) -> bool {
    if agent_skill_path == canonical_skill_path {
        return true;
    }

    let Some(agent_parent) = agent_skill_path.parent() else {
        return false;
    };
    let Some(agent_name) = agent_skill_path.file_name() else {
        return false;
    };

    let resolved_parent = agent_parent
        .canonicalize()
        .unwrap_or_else(|_| agent_parent.to_path_buf());
    let resolved_agent_path = resolved_parent.join(agent_name);

    canonical_skill_path
        .canonicalize()
        .map(|canonical| canonical == resolved_agent_path)
        .unwrap_or(false)
}

fn create_symlink_install(
    canonical_skill_path: &Path,
    agent_skill_path: &Path,
) -> Result<(), CliError> {
    if let Some(parent) = agent_skill_path.parent() {
        fs::create_dir_all(parent).map_err(|_| SkillsError::CreateDirFailed)?;
    }
    remove_existing_path(agent_skill_path)?;
    create_symlink_dir(canonical_skill_path, agent_skill_path)
        .map_err(|_| SkillsError::SymlinkFailed)?;
    Ok(())
}

#[cfg(unix)]
fn create_symlink_dir(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_symlink_dir(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[cfg(not(any(unix, windows)))]
fn create_symlink_dir(_target: &Path, _link: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "symlinks are not supported on this platform",
    ))
}

fn sanitize_skill_name(name: &str) -> String {
    let mut output = String::new();
    let mut last_was_dash = false;

    for ch in name.to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch);
            last_was_dash = false;
        } else if !last_was_dash {
            output.push('-');
            last_was_dash = true;
        }
    }

    let trimmed = output.trim_matches(|c| c == '-' || c == '.').to_string();
    if trimmed.is_empty() {
        DEFAULT_SKILL_NAME.to_string()
    } else {
        trimmed.chars().take(64).collect()
    }
}

fn safe_join(base: &Path, relative: &Path) -> Result<PathBuf, CliError> {
    let mut result = PathBuf::from(base);

    for component in relative.components() {
        match component {
            Component::Normal(part) => result.push(part),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(SkillsError::UnsafeInstallPath.into())
            }
        }
    }

    Ok(result)
}

impl InstallContext {
    fn current() -> Result<Self, CliError> {
        let cwd = env::current_dir().map_err(|_| SkillsError::CurrentDirUnavailable)?;
        let home = dirs::home_dir().ok_or(UtilsError::HomeDirNotFound)?;
        let xdg_config = dirs::config_dir().unwrap_or_else(|| home.join(".config"));
        Ok(Self {
            cwd,
            home,
            xdg_config,
        })
    }
}

impl AgentDefinition {
    fn global_dir(&self, context: &InstallContext) -> Option<PathBuf> {
        match self.global_dir {
            GlobalDirKind::HomeDotSkills(dir) => Some(context.home.join(dir).join("skills")),
            GlobalDirKind::HomeAgentsSkills => Some(context.home.join(".agents/skills")),
            GlobalDirKind::XdgAgentsSkills => Some(context.xdg_config.join("agents/skills")),
            GlobalDirKind::Antigravity => Some(context.home.join(".gemini/antigravity/skills")),
            GlobalDirKind::Claude => {
                let base = env::var_os("CLAUDE_CONFIG_DIR")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| context.home.join(".claude"));
                Some(base.join("skills"))
            }
            GlobalDirKind::Openclaw => {
                let candidates = [
                    context.home.join(".openclaw/skills"),
                    context.home.join(".clawdbot/skills"),
                    context.home.join(".moltbot/skills"),
                ];
                for candidate in candidates {
                    if candidate.exists() {
                        return Some(candidate);
                    }
                }
                Some(context.home.join(".openclaw/skills"))
            }
            GlobalDirKind::Codex => {
                let base = env::var_os("CODEX_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| context.home.join(".codex"));
                Some(base.join("skills"))
            }
            GlobalDirKind::Cortex => Some(context.home.join(".snowflake/cortex/skills")),
            GlobalDirKind::Crush => Some(context.home.join(".config/crush/skills")),
            GlobalDirKind::Deepagents => Some(context.home.join(".deepagents/agent/skills")),
            GlobalDirKind::Gemini => Some(context.home.join(".gemini/skills")),
            GlobalDirKind::Goose => Some(context.xdg_config.join("goose/skills")),
            GlobalDirKind::Kimi => Some(context.home.join(".config/agents/skills")),
            GlobalDirKind::Opencode => Some(context.xdg_config.join("opencode/skills")),
            GlobalDirKind::Pi => Some(context.home.join(".pi/agent/skills")),
            GlobalDirKind::Windsurf => Some(context.home.join(".codeium/windsurf/skills")),
        }
    }

    fn is_installed(&self, context: &InstallContext) -> bool {
        match self.detection {
            DetectionKind::HomeDot(dir) => context.home.join(dir).exists(),
            DetectionKind::CwdOrHomeDot(dir) => {
                context.cwd.join(dir).exists() || context.home.join(dir).exists()
            }
            DetectionKind::Amp => context.xdg_config.join("amp").exists(),
            DetectionKind::Antigravity => context.home.join(".gemini/antigravity").exists(),
            DetectionKind::Claude => env::var_os("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| context.home.join(".claude"))
                .exists(),
            DetectionKind::Openclaw => {
                context.home.join(".openclaw").exists()
                    || context.home.join(".clawdbot").exists()
                    || context.home.join(".moltbot").exists()
            }
            DetectionKind::Codex => {
                env::var_os("CODEX_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| context.home.join(".codex"))
                    .exists()
                    || Path::new("/etc/codex").exists()
            }
            DetectionKind::Cortex => context.home.join(".snowflake/cortex").exists(),
            DetectionKind::Crush => context.home.join(".config/crush").exists(),
            DetectionKind::Goose => context.xdg_config.join("goose").exists(),
            DetectionKind::Opencode => context.xdg_config.join("opencode").exists(),
            DetectionKind::Pi => context.home.join(".pi/agent").exists(),
            DetectionKind::Replit => context.cwd.join(".replit").exists(),
            DetectionKind::Windsurf => context.home.join(".codeium/windsurf").exists(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = env::temp_dir().join(format!("chroma-cli-{}-{}", label, unique));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn normalized_output(term: &TestTerminal) -> String {
        let stripped: String = term
            .output
            .iter()
            .flat_map(|line| line.chars())
            .map(|c| {
                if "│┌┐└┘─".contains(c) {
                    ' '
                } else {
                    c
                }
            })
            .collect();
        stripped.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn test_context(root: &Path) -> InstallContext {
        let cwd = root.join("cwd");
        let home = root.join("home");
        let xdg = root.join("xdg");
        fs::create_dir_all(&cwd).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&xdg).unwrap();
        InstallContext {
            cwd,
            home,
            xdg_config: xdg,
        }
    }

    fn sample_files() -> Vec<RemoteSkillFile> {
        vec![
            RemoteSkillFile {
                relative_path: PathBuf::from("SKILL.md"),
                contents: b"# Hello".to_vec(),
            },
            RemoteSkillFile {
                relative_path: PathBuf::from("docs/guide.md"),
                contents: b"guide".to_vec(),
            },
        ]
    }

    fn sample_registry_skill() -> RegistrySkill {
        RegistrySkill {
            name: "chroma-cloud".to_string(),
            description: "Cloud skill".to_string(),
            path: "chroma-cloud/SKILL.md".to_string(),
            topics: vec![RegistryTopic {
                paths: BTreeMap::from([
                    (
                        "typescript".to_string(),
                        "chroma-cloud/querying/typescript.md".to_string(),
                    ),
                    (
                        "python".to_string(),
                        "chroma-cloud/querying/python.md".to_string(),
                    ),
                ]),
            }],
            general: vec![RegistryGeneralDoc {
                path: "chroma-cloud/cli.md".to_string(),
            }],
        }
    }

    #[test]
    fn sanitize_skill_name_removes_unsafe_characters() {
        assert_eq!(sanitize_skill_name("../Chroma Cloud!!"), "chroma-cloud");
        assert_eq!(sanitize_skill_name("..."), "skill");
    }

    #[test]
    fn safe_join_rejects_path_traversal() {
        let base = PathBuf::from("/tmp/base");
        assert!(safe_join(&base, Path::new("../oops")).is_err());
        assert!(safe_join(&base, Path::new("/oops")).is_err());
        assert!(safe_join(&base, Path::new("nested/file")).is_ok());
    }

    #[test]
    fn copy_mode_installs_files_into_agent_directory() {
        let root = temp_dir("copy");
        let context = test_context(&root);
        let files = sample_files();
        let skill = sample_registry_skill();
        let agent = AGENTS.iter().find(|agent| agent.id == "augment").unwrap();

        let results = install_skill_files(
            &skill,
            &files,
            &[agent],
            InstallScope::Project,
            InstallMode::Copy,
            &context,
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(context
            .cwd
            .join(".augment/skills/chroma-cloud/SKILL.md")
            .exists());
        assert!(context
            .cwd
            .join(".augment/skills/chroma-cloud/docs/guide.md")
            .exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn symlink_mode_writes_canonical_install_for_universal_agent() {
        let root = temp_dir("symlink");
        let context = test_context(&root);
        let files = sample_files();
        let skill = sample_registry_skill();
        let agent = AGENTS.iter().find(|agent| agent.id == "codex").unwrap();

        let results = install_skill_files(
            &skill,
            &files,
            &[agent],
            InstallScope::Project,
            InstallMode::Symlink,
            &context,
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(context
            .cwd
            .join(".agents/skills/chroma-cloud/SKILL.md")
            .exists());
        assert_eq!(results[0].install_kind, "canonical");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_agent_ids_deduplicates_and_validates() {
        let agents = resolve_agent_ids(vec![
            "codex".to_string(),
            "codex".to_string(),
            "cursor".to_string(),
        ])
        .unwrap();
        assert_eq!(agents.len(), 2);
        assert!(resolve_agent_ids(vec!["universal".to_string()]).is_err());
    }

    #[test]
    fn resolve_scope_prints_selected_scope_summary() {
        let mut term = TestTerminal::new().with_inputs(vec!["1"]);

        let scope = resolve_scope(None, &mut term).unwrap();

        assert_eq!(scope, InstallScope::Global);
        assert!(term
            .output
            .iter()
            .any(|line| line.contains("Choose install scope")));
        assert!(term
            .output
            .iter()
            .any(|line| line.contains("Installation scope")));
        assert!(term.output.iter().any(|line| line.contains("Global")));
    }

    #[test]
    fn resolve_mode_prints_selected_method_summary() {
        let mut term = TestTerminal::new();

        let mode = resolve_mode(Some(InstallMode::Symlink), &mut term).unwrap();

        assert_eq!(mode, InstallMode::Symlink);
        assert!(term
            .output
            .iter()
            .any(|line| line.contains("Installation method")));
        assert!(term
            .output
            .iter()
            .any(|line| line.contains("Symlink (canonical shared install)")));
    }

    #[test]
    fn project_picker_includes_universal_agents_and_selected_additional_agents() {
        let root = temp_dir("project-picker");
        let context = test_context(&root);
        let additional_agents = AGENTS
            .iter()
            .filter(|agent| agent.selectable && !agent.universal)
            .collect::<Vec<_>>();
        let augment_index = additional_agents
            .iter()
            .position(|agent| agent.id == "augment")
            .unwrap();
        let augment_input = augment_index.to_string();
        let mut term = TestTerminal::new().with_inputs(vec![augment_input.as_str()]);

        let agents =
            resolve_agents(vec![], false, InstallScope::Project, &context, &mut term).unwrap();

        let universal_count = AGENTS
            .iter()
            .filter(|agent| agent.selectable && agent.universal)
            .count();

        assert_eq!(agents.len(), universal_count + 1);
        assert!(agents.iter().any(|agent| agent.id == "codex"));
        assert!(agents.iter().any(|agent| agent.id == "augment"));
        assert!(!agents.iter().any(|agent| agent.id == "claude-code"));
        assert!(term.output.iter().any(|line| line.contains("Agents")));
        assert!(term.output.iter().any(|line| line.contains("Amp")));
        assert!(term.output.iter().any(|line| line.contains("Augment")));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn project_picker_defaults_to_claude_code() {
        let root = temp_dir("project-picker-default");
        let context = test_context(&root);
        let mut term = TestTerminal::new().with_inputs(vec![""]);

        let agents =
            resolve_agents(vec![], false, InstallScope::Project, &context, &mut term).unwrap();

        let universal_count = AGENTS
            .iter()
            .filter(|agent| agent.selectable && agent.universal)
            .count();

        assert_eq!(agents.len(), universal_count + 1);
        assert!(agents.iter().any(|agent| agent.id == "claude-code"));
        assert!(term.output.iter().any(|line| line.contains("Agents")));
        assert!(normalized_output(&term).contains("Claude Code"));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn project_picker_skips_detected_agents_shortcut() {
        let root = temp_dir("project-picker-detected");
        let context = test_context(&root);
        fs::create_dir_all(context.home.join(".claude")).unwrap();

        let additional_agents = AGENTS
            .iter()
            .filter(|agent| agent.selectable && !agent.universal)
            .collect::<Vec<_>>();
        let augment_index = additional_agents
            .iter()
            .position(|agent| agent.id == "augment")
            .unwrap();
        let augment_input = augment_index.to_string();
        let mut term = TestTerminal::new().with_inputs(vec![augment_input.as_str()]);

        let agents =
            resolve_agents(vec![], false, InstallScope::Project, &context, &mut term).unwrap();

        assert!(!agents.iter().any(|agent| agent.id == "claude-code"));
        assert!(agents.iter().any(|agent| agent.id == "augment"));
        assert!(term.output.iter().any(|line| line.contains("Agents")));
        assert!(term.output.iter().any(|line| line.contains("Augment")));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn all_detected_flag_still_uses_detected_agents() {
        let root = temp_dir("all-detected");
        let context = test_context(&root);
        fs::create_dir_all(context.home.join(".claude")).unwrap();
        fs::create_dir_all(context.home.join(".cursor")).unwrap();
        let mut term = TestTerminal::new();

        let agents =
            resolve_agents(vec![], true, InstallScope::Project, &context, &mut term).unwrap();

        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].id, "claude-code");
        assert_eq!(agents[1].id, "cursor");
        assert!(term.output.iter().any(|line| line.contains("Agents")));
        assert!(term.output.iter().any(|line| line.contains("Cursor")));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn global_picker_requires_at_least_one_selection() {
        let root = temp_dir("global-picker-empty");
        let context = test_context(&root);
        let mut term = TestTerminal::new().with_inputs(vec![""]);

        let result = resolve_agents(vec![], false, InstallScope::Global, &context, &mut term);

        assert!(matches!(
            result,
            Err(CliError::Skills(SkillsError::NoAgentsSelected))
        ));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn global_picker_resolves_selected_agents() {
        let root = temp_dir("global-picker");
        let context = test_context(&root);
        let selectable_agents = AGENTS
            .iter()
            .filter(|agent| agent.selectable)
            .collect::<Vec<_>>();
        let codex_index = selectable_agents
            .iter()
            .position(|agent| agent.id == "codex")
            .unwrap();
        let augment_index = selectable_agents
            .iter()
            .position(|agent| agent.id == "augment")
            .unwrap();
        let input = format!("{codex_index},{augment_index}");
        let mut term = TestTerminal::new().with_inputs(vec![input.as_str()]);

        let agents =
            resolve_agents(vec![], false, InstallScope::Global, &context, &mut term).unwrap();

        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].id, "codex");
        assert_eq!(agents[1].id, "augment");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn registry_file_paths_include_declared_skill_files() {
        let skill = sample_registry_skill();
        let files = registry_file_paths(&skill).unwrap();

        assert_eq!(
            files,
            vec![
                "chroma-cloud/SKILL.md".to_string(),
                "chroma-cloud/cli.md".to_string(),
                "chroma-cloud/querying/python.md".to_string(),
                "chroma-cloud/querying/typescript.md".to_string(),
            ]
        );
        assert_eq!(
            skill_relative_path(&skill, "chroma-cloud/querying/typescript.md").unwrap(),
            PathBuf::from("querying/typescript.md")
        );
    }
}
