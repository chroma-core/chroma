from typing import Any, Dict

import yaml


def set_log_file_path(
    log_config_path: str, new_filename: str = "chroma.log"
) -> Dict[str, Any]:
    """This works with the standard log_config.yml file.
    It will not work with custom log configs that may use different handlers"""
    with open(f"{log_config_path}", "r") as file:
        log_config = yaml.safe_load(file)
    for handler in log_config["handlers"].values():
        if handler.get("class") == "logging.handlers.RotatingFileHandler":
            handler["filename"] = new_filename

    return log_config
