use super::agents::{AgentDefinition, InstallContext, AGENTS};
use super::registry::{
    fetch_skills_registry, registry_file_paths, skill_relative_path, RegistrySkill, SkillsRegistry,
    SKILLS_RAW_BASE_URL, SKILLS_REGISTRY_URL,
};
use super::SkillsError;
use crate::terminal::Terminal;
use crate::tui::widgets::summary_panel::print_summary_panel;
use crate::tui::widgets::{
    print_status_line, print_success_banner, FilterableMultiSelectPrompt, FilterableSelectItem,
    PanelSelectPrompt,
};
use crate::utils::CliError;
use clap::{Parser, ValueEnum};
use colored::Colorize;
use reqwest::header::USER_AGENT;
use reqwest::Client;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

const DEFAULT_SKILL_NAME: &str = "skill";

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
    pub(crate) fn for_init(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            scope: Some(InstallScope::Project),
            mode: Some(InstallMode::Symlink),
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

#[derive(Debug, Clone)]
struct RemoteSkillFile {
    relative_path: PathBuf,
    contents: Vec<u8>,
}

#[derive(Debug)]
struct InstallResult {
    #[cfg_attr(not(test), allow(dead_code))]
    install_kind: String,
}

pub(crate) async fn install_skill(
    args: InstallSkillArgs,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let context = InstallContext::current()?;
    let client = Client::new();
    let registry = fetch_skills_registry(&client).await?;
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

    let files = download_skill_files(&client, skill).await?;
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

async fn download_skill_files(
    client: &Client,
    skill: &RegistrySkill,
) -> Result<Vec<RemoteSkillFile>, CliError> {
    let mut files = Vec::new();

    for remote_path in registry_file_paths(skill)? {
        let url = format!("{SKILLS_RAW_BASE_URL}{remote_path}");
        let download_failed = || SkillsError::SkillDownloadFailed {
            skill: skill.name.clone(),
            file: remote_path.clone(),
        };
        let bytes = client
            .get(url)
            .header(USER_AGENT, "chroma-cli")
            .send()
            .await
            .map_err(|_| download_failed())?
            .error_for_status()
            .map_err(|_| download_failed())?
            .bytes()
            .await
            .map_err(|_| download_failed())?;

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

pub(super) fn safe_join(base: &Path, relative: &Path) -> Result<PathBuf, CliError> {
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

#[cfg(test)]
mod tests {
    use super::super::registry::sample_registry_skill;
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;
    use tempfile::TempDir;

    fn temp_dir(label: &str) -> TempDir {
        TempDir::with_prefix(format!("chroma-cli-{}-", label)).unwrap()
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
        let context = test_context(root.path());
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
    }

    #[test]
    fn symlink_mode_writes_canonical_install_for_universal_agent() {
        let root = temp_dir("symlink");
        let context = test_context(root.path());
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
        let context = test_context(root.path());
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
    }

    #[test]
    fn project_picker_defaults_to_claude_code() {
        let root = temp_dir("project-picker-default");
        let context = test_context(root.path());
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
    }

    #[test]
    fn project_picker_skips_detected_agents_shortcut() {
        let root = temp_dir("project-picker-detected");
        let context = test_context(root.path());
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
    }

    #[test]
    fn all_detected_flag_still_uses_detected_agents() {
        let root = temp_dir("all-detected");
        let context = test_context(root.path());
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
    }

    #[test]
    fn global_picker_requires_at_least_one_selection() {
        let root = temp_dir("global-picker-empty");
        let context = test_context(root.path());
        let mut term = TestTerminal::new().with_inputs(vec![""]);

        let result = resolve_agents(vec![], false, InstallScope::Global, &context, &mut term);

        assert!(matches!(
            result,
            Err(CliError::Skills(SkillsError::NoAgentsSelected))
        ));
    }

    #[test]
    fn global_picker_resolves_selected_agents() {
        let root = temp_dir("global-picker");
        let context = test_context(root.path());
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
    }
}
