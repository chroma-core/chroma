import os
import random
import gc
import time


# Borrowed from https://github.com/rogerbinns/apsw/blob/master/apsw/tests.py#L224
# Used to delete files on Windows that are in use, since Windows file locking
# behaves differently to other operating systems
# This should only be used for test or non-production code, such as in reset_state.
def delete_file(name: str) -> None:
    try:
        os.remove(name)
    except Exception:
        pass
    chars = list("abcdefghijklmn")
    random.shuffle(chars)
    newname = name + "-n-" + "".join(chars)
    count = 0
    while os.path.exists(name):
        count += 1
        try:
            os.rename(name, newname)
        except Exception:
            if count > 30:  # 3 seconds we have been at this!
                # So give up and give it a stupid name.  The sooner
                # this so called operating system withers into obscurity
                # the better
                n = list("abcdefghijklmnopqrstuvwxyz")
                random.shuffle(n)
                final_name = "".join(n)
                try:
                    os.rename(name, "windowssucks-" + final_name + ".deletememanually")
                except Exception:
                    pass
                break
            # Make windows happy
            time.sleep(0.1)
            gc.collect()
    if os.path.exists(newname):
        # bgdelq.put(newname)
        # Give bg thread a chance to run
        time.sleep(0.1)
