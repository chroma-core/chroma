# An implementation of https://en.wikipedia.org/wiki/Rendezvous_hashing
from typing import Callable, cast

Hasher = Callable[[str, str], int]
Member = str
Members = list[str]
Key = str


def assign(key: Key, members: Members, hasher: Hasher) -> Member:
    """Assigns a key to a member using the rendezvous hashing algorithm"""
    if len(members) == 0:
        raise ValueError("Cannot assign key to empty memberlist")
    if len(members) == 1:
        return members[0]
    if key == "":
        raise ValueError("Cannot assign empty key")

    max_score = -1
    max_member = None

    for member in members:
        score = hasher(member, key)
        if score > max_score:
            max_score = score
            max_member = member

    max_member = cast(Member, max_member)
    return max_member
