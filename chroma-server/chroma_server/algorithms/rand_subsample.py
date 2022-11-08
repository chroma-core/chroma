import random


def rand_bisectional_subsample(data):
    """
    Randomly bisectionally subsample a list of data to size.
    """
    return data.sample(frac=0.5, replace=True, random_state=1)
