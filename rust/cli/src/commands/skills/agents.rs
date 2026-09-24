use super::SkillsError;
use crate::utils::{CliError, UtilsError};
use std::env;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub(super) struct AgentDefinition {
    pub(super) id: &'static str,
    pub(super) display_name: &'static str,
    pub(super) universal: bool,
    pub(super) project_dir: &'static str,
    pub(super) global_dir: GlobalDirKind,
    pub(super) detection: DetectionKind,
    pub(super) selectable: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum GlobalDirKind {
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
pub(super) enum DetectionKind {
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
pub(super) struct InstallContext {
    pub(super) cwd: PathBuf,
    pub(super) home: PathBuf,
    pub(super) xdg_config: PathBuf,
}

impl InstallContext {
    pub(super) fn current() -> Result<Self, CliError> {
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
    pub(super) fn global_dir(&self, context: &InstallContext) -> Option<PathBuf> {
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

    pub(super) fn is_installed(&self, context: &InstallContext) -> bool {
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

pub(super) const AGENTS: &[AgentDefinition] = &[
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
