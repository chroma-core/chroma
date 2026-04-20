use crate::style;
use crate::utils::{CliError, UtilsError};
use arboard::Clipboard;
use clap::ValueEnum;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{cursor, event, ExecutableCommand};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{stdout, Write};

pub const LOGO: &str = "
                \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####
             \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########
           \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########
         \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############
           \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############
             \x1b[38;5;069m((((((((\x1b[38;5;203m((((((((\x1b[38;5;220m##############
                \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[0m
";

pub const HOLIDAY_LOGO: &str = "
\x1b[97m  *       .                                     *              .
  .                 \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####\x1b[97m              *
    *            \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########\x1b[97m    .
  .            \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########\x1b[97m        *
      *      \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############\x1b[97m  .
  *         \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############\x1b[97m
    .       \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############\x1b[97m     *
  .          \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############\x1b[97m .
      *      \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############\x1b[97m      *
  *            \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############\x1b[97m   .
  .      *         \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[97m          .
      .        *                        .        *           .\x1b[0m
";

pub enum ColorLevel {
    Ansi256,
    TrueColor,
}

#[derive(Debug, Clone, Default, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
pub enum Theme {
    #[default]
    Dark,
    Light,
}

fn write_secret_prompt(
    stdout: &mut std::io::Stdout,
    prompt: &str,
    password_len: usize,
) -> io::Result<()> {
    stdout.write_all(prompt.as_bytes())?;
    stdout.write_all(b": ")?;
    if password_len > 0 {
        stdout.write_all("*".repeat(password_len).as_bytes())?;
    }
    stdout.flush()?;
    Ok(())
}

pub fn read_secret(prompt: &str) -> io::Result<String> {
    let mut stdout = stdout();
    let mut password = String::new();

    write_secret_prompt(&mut stdout, prompt, password.len())?;

    enable_raw_mode()?;

    loop {
        if let Event::Key(KeyEvent {
            code, modifiers, ..
        }) = event::read()?
        {
            if modifiers == KeyModifiers::CONTROL && code == KeyCode::Char('c') {
                disable_raw_mode()?;
                stdout.write_all(b"\n")?;
                stdout.flush()?;
                return Err(io::Error::new(io::ErrorKind::Interrupted, "interrupted"));
            }

            match code {
                KeyCode::Enter => break,
                KeyCode::Char(c) => {
                    password.push(c);
                    stdout.write_all(b"*")?;
                }
                KeyCode::Backspace => {
                    if !password.is_empty() {
                        password.pop();
                        stdout.execute(cursor::MoveLeft(1))?;
                        stdout.write_all(b" ")?;
                        stdout.execute(cursor::MoveLeft(1))?;
                    }
                }
                _ => {}
            }
            stdout.flush()?;
        }
    }

    disable_raw_mode()?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;

    Ok(password)
}

pub fn validate_uri(input: String) -> Result<String, UtilsError> {
    if input.is_empty() {
        return Err(UtilsError::InvalidName);
    }

    let re = Regex::new(r"^[a-zA-Z0-9_-]+$")
        .map_err(|e| e.to_string())
        .map_err(|_| UtilsError::NameValidationFailed)?;
    if !re.is_match(&input) {
        return Err(UtilsError::InvalidName);
    }

    Ok(input)
}

pub fn copy_to_clipboard(copy_string: &str) -> Result<(), CliError> {
    let mut clipboard = Clipboard::new().map_err(|_| UtilsError::CopyToClipboardFailed)?;
    clipboard
        .set_text(copy_string)
        .map_err(|_| UtilsError::CopyToClipboardFailed)?;
    println!("\n{}", style::accent_bold("Copied to clipboard!"));
    Ok(())
}
