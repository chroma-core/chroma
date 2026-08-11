use crate::utils::{CliError, UtilsError};
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};

pub trait Terminal {
    fn println(&mut self, msg: &str);
    fn prompt_input(&mut self) -> Result<String, CliError>;
    fn prompt_select(&mut self, items: &[String]) -> Result<usize, CliError>;
    fn prompt_confirm(&mut self, prompt: &str) -> Result<bool, CliError>;
}

pub struct SystemTerminal;

impl Terminal for SystemTerminal {
    fn println(&mut self, msg: &str) {
        println!("{}", msg);
    }

    fn prompt_input(&mut self) -> Result<String, CliError> {
        let input: String = Input::with_theme(&ColorfulTheme::default())
            .interact_text()
            .map_err(|_| UtilsError::UserInputFailed)?;
        Ok(input)
    }

    fn prompt_select(&mut self, items: &[String]) -> Result<usize, CliError> {
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(items)
            .default(0)
            .interact()
            .map_err(|_| UtilsError::UserInputFailed)?;
        Ok(selection)
    }

    fn prompt_confirm(&mut self, prompt: &str) -> Result<bool, CliError> {
        let confirmed = Confirm::new()
            .with_prompt(prompt)
            .interact()
            .map_err(|_| UtilsError::UserInputFailed)?;
        Ok(confirmed)
    }
}

#[cfg(test)]
pub mod test_terminal {
    use super::Terminal;
    use crate::utils::{CliError, UtilsError};

    pub struct TestTerminal {
        pub output: Vec<String>,
        inputs: Vec<String>,
        input_index: usize,
    }

    impl TestTerminal {
        pub fn new() -> Self {
            Self {
                output: Vec::new(),
                inputs: Vec::new(),
                input_index: 0,
            }
        }

        pub fn with_inputs(mut self, inputs: Vec<&str>) -> Self {
            self.inputs = inputs.into_iter().map(|s| s.to_string()).collect();
            self
        }

        fn next_input(&mut self) -> Result<String, CliError> {
            if self.input_index < self.inputs.len() {
                let input = self.inputs[self.input_index].clone();
                self.input_index += 1;
                Ok(input)
            } else {
                Err(UtilsError::UserInputFailed.into())
            }
        }
    }

    impl Terminal for TestTerminal {
        fn println(&mut self, msg: &str) {
            self.output.push(msg.to_string());
        }

        fn prompt_input(&mut self) -> Result<String, CliError> {
            self.next_input()
        }

        fn prompt_select(&mut self, _items: &[String]) -> Result<usize, CliError> {
            let input = self.next_input()?;
            input
                .parse::<usize>()
                .map_err(|_| UtilsError::UserInputFailed.into())
        }

        fn prompt_confirm(&mut self, _prompt: &str) -> Result<bool, CliError> {
            let input = self.next_input()?;
            Ok(input.to_lowercase() == "y" || input.to_lowercase() == "yes")
        }
    }
}
