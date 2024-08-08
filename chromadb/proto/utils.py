import json
from typing import Any, List, Tuple


def get_default_grpc_options() -> List[Tuple[str, Any]]:
    service_config_str = json.dumps(
        {
            "methodConfig": [
                {
                    "name": [{}],
                    "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "0.1s",
                        "maxBackoff": "1s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": ["UNAVAILABLE", "UNKNOWN"],
                    },
                }
            ]
        }
    )

    return [
        ("grpc.enable_retries", 1),
        ("grpc.service_config", service_config_str),
    ]
