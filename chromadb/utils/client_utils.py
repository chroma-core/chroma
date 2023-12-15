def compare_versions(version1: str, version2: str) -> int:
    """Compares two versions of the format X.Y.Z and returns 1 if version1 is greater than version2, -1 if version1 is
    less than version2, and 0 if version1 is equal to version2.
    """
    v1_components = list(map(int, version1.split(".")))
    v2_components = list(map(int, version2.split(".")))

    for v1, v2 in zip(v1_components, v2_components):
        if v1 > v2:
            return 1
        elif v1 < v2:
            return -1

    if len(v1_components) > len(v2_components):
        return 1
    elif len(v1_components) < len(v2_components):
        return -1

    return 0
