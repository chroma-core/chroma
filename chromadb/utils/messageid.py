import pulsar


def pulsar_to_int(message_id: pulsar.MessageId) -> int:
    ledger_id: int = message_id.ledger_id()
    entry_id: int = message_id.entry_id()
    batch_index: int = message_id.batch_index()
    partition: int = message_id.partition()

    return ledger_id << 128 | entry_id << 64 | batch_index << 32 | partition


def int_to_pulsar(message_id_int: int) -> pulsar.MessageId:
    return pulsar.MessageId(
        message_id_int & 0xFFFFFFFF,
        (message_id_int >> 128) & 0xFFFFFFFFFFFFFFFF,
        (message_id_int >> 64) & 0xFFFFFFFFFFFFFFFF,
        (message_id_int >> 32) & 0xFFFFFFFF,
    )


def int_to_bytes(int: int) -> bytes:
    """Convert int to a 24 byte big endian byte string"""
    return int.to_bytes(24, "big")


def bytes_to_int(bytes: bytes) -> int:
    """Convert a 24 byte big endian byte string to an int"""
    return int.from_bytes(bytes, "big")


# Sorted in lexographic order
base85 = (
    "!#$%&()*+-0123456789;<=>?@ABCDEFGHIJKLMNOP"
    + "QRSTUVWXYZ^_`abcdefghijklmnopqrstuvwxyz{|}~"
)


# not the most efficient way to do this, see benchmark function below
def _int_to_str(n: int) -> str:
    if n < 85:
        return base85[n]
    else:
        return _int_to_str(n // 85) + base85[n % 85]


def int_to_str(n: int) -> str:
    return _int_to_str(n).rjust(36, "!")  # left pad with '!' to 36 chars


def str_to_int(s: str) -> int:
    return sum(base85.index(c) * 85**i for i, c in enumerate(s[::-1]))


# 1m in 5 seconds on a M1 Pro
# Not fast, but not likely to be a bottleneck either
def _benchmark() -> None:
    import random
    import time

    t0 = time.time()
    for i in range(1000000):
        x = random.randint(0, 2**192 - 1)
        s = int_to_str(x)
        if s == "!":  # prevent compiler from optimizing out
            print("oops")
    t1 = time.time()
    print(t1 - t0)
