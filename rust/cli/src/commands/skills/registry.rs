use super::SkillsError;
use crate::utils::CliError;
use reqwest::header::USER_AGENT;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

pub(super) const SKILLS_REGISTRY_URL: &str =
    "https://raw.githubusercontent.com/chroma-core/agent-skills/main/skills/registry.json";
pub(super) const SKILLS_RAW_BASE_URL: &str =
    "https://raw.githubusercontent.com/chroma-core/agent-skills/main/skills/";

#[derive(Debug, Deserialize)]
pub(super) struct SkillsRegistry {
    pub(super) skills: Vec<RegistrySkill>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RegistrySkill {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) path: String,
    #[serde(default)]
    pub(super) topics: Vec<RegistryTopic>,
    #[serde(default)]
    pub(super) general: Vec<RegistryGeneralDoc>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RegistryTopic {
    pub(super) paths: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RegistryGeneralDoc {
    pub(super) path: String,
}

pub(super) async fn fetch_skills_registry(client: &Client) -> Result<SkillsRegistry, CliError> {
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

pub(super) fn registry_file_paths(skill: &RegistrySkill) -> Result<Vec<String>, CliError> {
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
    validate_registry_relative_path(skill, skill_root)?;
    validate_registry_relative_path(skill, relative)?;
    Ok(path.to_string_lossy().into_owned())
}

fn validate_registry_relative_path(skill: &RegistrySkill, path: &Path) -> Result<(), CliError> {
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(SkillsError::InvalidRegistryEntry(skill.name.clone()).into())
            }
        }
    }
    Ok(())
}

pub(super) fn skill_relative_path(
    skill: &RegistrySkill,
    remote_path: &str,
) -> Result<PathBuf, CliError> {
    let skill_root = skill_root_dir(skill)?;
    Path::new(remote_path)
        .strip_prefix(&skill_root)
        .map(Path::to_path_buf)
        .map_err(|_| SkillsError::InvalidRegistryEntry(skill.name.clone()).into())
}

#[cfg(test)]
pub(super) fn sample_registry_skill() -> RegistrySkill {
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn registry_file_paths_reject_path_traversal() {
        let mut skill = sample_registry_skill();
        skill.general = vec![RegistryGeneralDoc {
            path: "chroma-cloud/../evil.md".to_string(),
        }];

        let err = registry_file_paths(&skill).unwrap_err();

        assert!(matches!(
            err,
            CliError::Skills(SkillsError::InvalidRegistryEntry(_))
        ));
    }
}
