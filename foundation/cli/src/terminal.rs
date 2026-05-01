use crate::error::FoundationError;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};

pub trait Terminal {
    fn println(&mut self, msg: &str);
    #[allow(dead_code)]
    fn eprintln(&mut self, msg: &str);
    fn prompt_input(&mut self) -> Result<String, FoundationError>;
    fn prompt_select(&mut self, items: &[String]) -> Result<usize, FoundationError>;
    #[allow(dead_code)]
    fn prompt_confirm(&mut self, prompt: &str) -> Result<bool, FoundationError>;
}

pub struct SystemTerminal;

impl Terminal for SystemTerminal {
    fn println(&mut self, msg: &str) {
        println!("{}", msg);
    }

    fn eprintln(&mut self, msg: &str) {
        eprintln!("{}", msg);
    }

    fn prompt_input(&mut self) -> Result<String, FoundationError> {
        let input: String = Input::with_theme(&ColorfulTheme::default())
            .interact_text()
            .map_err(|_| FoundationError::UserInputFailed)?;
        Ok(input)
    }

    fn prompt_select(&mut self, items: &[String]) -> Result<usize, FoundationError> {
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(items)
            .default(0)
            .interact()
            .map_err(|_| FoundationError::UserInputFailed)?;
        Ok(selection)
    }

    fn prompt_confirm(&mut self, prompt: &str) -> Result<bool, FoundationError> {
        let confirmed = Confirm::new()
            .with_prompt(prompt)
            .interact()
            .map_err(|_| FoundationError::UserInputFailed)?;
        Ok(confirmed)
    }
}

#[cfg(test)]
pub mod test_terminal {
    use super::Terminal;
    use crate::error::FoundationError;

    pub struct TestTerminal {
        pub output: Vec<String>,
        #[allow(dead_code)]
        pub err_output: Vec<String>,
        inputs: Vec<String>,
        input_index: usize,
    }

    impl TestTerminal {
        pub fn new() -> Self {
            Self {
                output: Vec::new(),
                err_output: Vec::new(),
                inputs: Vec::new(),
                input_index: 0,
            }
        }

        pub fn with_inputs(mut self, inputs: Vec<&str>) -> Self {
            self.inputs = inputs.into_iter().map(|s| s.to_string()).collect();
            self
        }

        fn next_input(&mut self) -> Result<String, FoundationError> {
            if self.input_index < self.inputs.len() {
                let input = self.inputs[self.input_index].clone();
                self.input_index += 1;
                Ok(input)
            } else {
                Err(FoundationError::UserInputFailed)
            }
        }
    }

    impl Terminal for TestTerminal {
        fn println(&mut self, msg: &str) {
            self.output.push(msg.to_string());
        }

        fn eprintln(&mut self, msg: &str) {
            self.err_output.push(msg.to_string());
        }

        fn prompt_input(&mut self) -> Result<String, FoundationError> {
            self.next_input()
        }

        fn prompt_select(&mut self, _items: &[String]) -> Result<usize, FoundationError> {
            let input = self.next_input()?;
            input
                .parse::<usize>()
                .map_err(|_| FoundationError::UserInputFailed)
        }

        fn prompt_confirm(&mut self, _prompt: &str) -> Result<bool, FoundationError> {
            let input = self.next_input()?;
            Ok(input.to_lowercase() == "y" || input.to_lowercase() == "yes")
        }
    }
}
