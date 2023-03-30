import pulsar


def pulsar_to_int(message_id):
    return (message_id.ledger_id() << 96) + (message_id.entry_id() << 32) + message_id.batch_index()


def int_to_pulsar(message_id_int):
    return pulsar.MessageId(
        0,
        (message_id_int >> 96) & 0xFFFFFFFFFFFFFFFF,
        (message_id_int >> 32) & 0xFFFFFFFFFFFFFFFF,
        message_id_int & 0xFFFFFFFF,
    )


def int_to_bytes(int: int) -> bytes:
    return int.to_bytes(20, "big")


def bytes_to_int(bytes: bytes):
    return int.from_bytes(bytes, "big")


# Sorted in lexographic order
base85 = "!#$%&()*+-0123456789;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ^_`abcdefghijklmnopqrstuvwxyz{|}~"

# by far not the most efficient way to do this
def _int_to_str(n: int) -> str:
    if n < 85:
        return base85[n]
    else:
        return _int_to_str(n // 85) + base85[n % 85]


def int_to_str(n: int) -> str:
    return _int_to_str(n).rjust(25, "!")


def str_to_int(s: str) -> int:
    return sum(base85.index(c) * 85**i for i, c in enumerate(s[::-1]))


def _benchmark():
    import random
    import time

    t0 = time.time()
    for i in range(1000000):
        x = random.randint(0, 2**160 - 1)
        if int_to_str(x) == "!":  # prevent compiler from optimizing out
            print("oops")
    t1 = time.time()
    print(t1 - t0)
