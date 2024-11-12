# An implementation of https://en.wikipedia.org/wiki/Rendezvous_hashing
from typing import Callable, List, cast
import mmh3

Hasher = Callable[[str, str], int]
Member = str
Members = List[str]
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


def merge_hashes(x: int, y: int) -> int:
    """murmurhash3 mix 64-bit"""
    acc = x ^ y
    acc ^= acc >> 33
    acc = (
        acc * 0xFF51AFD7ED558CCD
    ) % 2**64  # We need to mod here to prevent python from using arbitrary size int
    acc ^= acc >> 33
    acc = (acc * 0xC4CEB9FE1A85EC53) % 2**64
    acc ^= acc >> 33
    return acc


def murmur3hasher(member: Member, key: Key) -> int:
    """Hashes the key and member using the murmur3 hashing algorithm"""
    member_hash = mmh3.hash64(member, signed=False)[0]
    key_hash = mmh3.hash64(key, signed=False)[0]
    return merge_hashes(member_hash, key_hash)
