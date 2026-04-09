use crate::utils::{CliError, UtilsError};
use dialoguer::theme::ColorfulTheme;
use dialoguer::Input;

pub trait Terminal {
    fn println(&mut self, msg: &str);
    fn prompt_input(&mut self) -> Result<String, CliError>;
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
    }

    impl Terminal for TestTerminal {
        fn println(&mut self, msg: &str) {
            self.output.push(msg.to_string());
        }

        fn prompt_input(&mut self) -> Result<String, CliError> {
            if self.input_index < self.inputs.len() {
                let input = self.inputs[self.input_index].clone();
                self.input_index += 1;
                Ok(input)
            } else {
                Err(UtilsError::UserInputFailed.into())
            }
        }
    }
}
