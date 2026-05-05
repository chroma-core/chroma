//! Static catalogue of cookie-store paths for the long-tail browsers
//! rookie does NOT auto-enumerate (Atlas, Sidekick, Comet, Dia,
//! Wavebox, Yandex variants, snap/flatpak Firefox containers, etc.).
//!
//! When `login` umbrella scan misses everything, we print this table so
//! the user can copy-paste a path into `--cookie-file <path>`. The
//! escape hatch ultimately calls `rookie::any_browser(path, ...)` which
//! sniffs the schema (Chromium-style sqlite vs Firefox cookies.sqlite).

/// One entry in the printable hint table.
pub struct LongTailHint {
    pub browser: &'static str,
    pub os: &'static str,
    pub path: &'static str,
}

#[allow(dead_code)]
pub fn long_tail_hints() -> &'static [LongTailHint] {
    LONG_TAIL_HINTS
}

const LONG_TAIL_HINTS: &[LongTailHint] = &[
    // ---- macOS ----
    LongTailHint {
        browser: "Atlas (~/Library/Application Support/Atlas/Default/Cookies)",
        os: "macOS",
        path: "~/Library/Application Support/Atlas/Default/Cookies",
    },
    LongTailHint {
        browser: "Atlas (alt path)",
        os: "macOS",
        path: "~/Library/Application Support/io.openatlas.atlas/Default/Cookies",
    },
    LongTailHint {
        browser: "Atlas (com.openai.atlas variant)",
        os: "macOS",
        path: "~/Library/Application Support/com.openai.atlas/Default/Cookies",
    },
    LongTailHint {
        browser: "Sidekick",
        os: "macOS",
        path: "~/Library/Application Support/Sidekick/Default/Cookies",
    },
    LongTailHint {
        browser: "Comet",
        os: "macOS",
        path: "~/Library/Application Support/Comet/Default/Cookies",
    },
    LongTailHint {
        browser: "Dia",
        os: "macOS",
        path: "~/Library/Application Support/Dia/Default/Cookies",
    },
    LongTailHint {
        browser: "Wavebox",
        os: "macOS",
        path: "~/Library/Application Support/Wavebox/Default/Cookies",
    },
    LongTailHint {
        browser: "Yandex",
        os: "macOS",
        path: "~/Library/Application Support/Yandex/YandexBrowser/Default/Cookies",
    },
    LongTailHint {
        browser: "DuckDuckGo",
        os: "macOS",
        path: "~/Library/Containers/com.duckduckgo.macos.browser/Data/Library/Application Support/DuckDuckGo/Cookies",
    },
    // ---- Linux ----
    LongTailHint {
        browser: "Yandex",
        os: "Linux",
        path: "~/.config/yandex-browser/Default/Cookies",
    },
    LongTailHint {
        browser: "Waterfox",
        os: "Linux",
        path: "~/.waterfox/<profile>/cookies.sqlite",
    },
    LongTailHint {
        browser: "Floorp",
        os: "Linux",
        path: "~/.floorp/<profile>/cookies.sqlite",
    },
    LongTailHint {
        browser: "LibreWolf (flatpak)",
        os: "Linux",
        path: "~/.var/app/io.gitlab.librewolf-community/.librewolf/<profile>/cookies.sqlite",
    },
    LongTailHint {
        browser: "Firefox (snap)",
        os: "Linux",
        path: "~/snap/firefox/common/.mozilla/firefox/<profile>/cookies.sqlite",
    },
    LongTailHint {
        browser: "Tor Browser",
        os: "Linux",
        path: "~/.tor-browser/Browser/TorBrowser/Data/Browser/profile.default/cookies.sqlite",
    },
    // ---- Windows ----
    LongTailHint {
        browser: "Yandex",
        os: "Windows",
        path: r"%LOCALAPPDATA%\Yandex\YandexBrowser\User Data\Default\Cookies",
    },
    LongTailHint {
        browser: "Waterfox",
        os: "Windows",
        path: r"%APPDATA%\Waterfox\Profiles\<profile>\cookies.sqlite",
    },
    LongTailHint {
        browser: "Floorp",
        os: "Windows",
        path: r"%APPDATA%\Floorp\Profiles\<profile>\cookies.sqlite",
    },
    LongTailHint {
        browser: "Tor Browser",
        os: "Windows",
        path: r"%USERPROFILE%\Desktop\Tor Browser\Browser\TorBrowser\Data\Browser\profile.default\cookies.sqlite",
    },
];

/// Print the table filtered to the current OS, with a header explaining
/// what the user is supposed to do with the paths.
pub fn print_for_current_os() {
    let host_os = if cfg!(target_os = "macos") {
        "macOS"
    } else if cfg!(target_os = "linux") {
        "Linux"
    } else if cfg!(target_os = "windows") {
        "Windows"
    } else {
        ""
    };
    let rows: Vec<&LongTailHint> = LONG_TAIL_HINTS
        .iter()
        .filter(|h| host_os.is_empty() || h.os == host_os)
        .collect();
    if rows.is_empty() {
        return;
    }
    println!();
    println!("If your browser isn't in rookie's default list, you can point");
    println!("`login --cookie-file <path>` at one of these directly:");
    println!();
    for h in rows {
        println!("  {:<32}  {}", h.browser, h.path);
    }
    println!();
}
