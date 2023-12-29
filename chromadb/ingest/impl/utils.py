import re
from typing import Tuple

topic_regex = r"persistent:\/\/(?P<tenant>.+)\/(?P<namespace>.+)\/(?P<topic>.+)"


def parse_topic_name(topic_name: str) -> Tuple[str, str, str]:
    """Parse the topic name into the tenant, namespace and topic name"""
    match = re.match(topic_regex, topic_name)
    if not match:
        raise ValueError(f"Invalid topic name: {topic_name}")
    return match.group("tenant"), match.group("namespace"), match.group("topic")


def create_pulsar_connection_str(host: str, port: str) -> str:
    return f"pulsar://{host}:{port}"


def create_topic_name(tenant: str, namespace: str, topic: str) -> str:
    return f"persistent://{tenant}/{namespace}/{topic}"
