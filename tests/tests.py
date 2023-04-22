import pandas as pd

from pipelines import Pipeline, Check, Transform
from pipelines.utils import contains_no_duplicates, contains_no_nulls, contains_columns, is_less_than


def add_one(data: pd.DataFrame):
    """
    Adds one to the current dataframe values.

    :param data:
    :return:
    """
    return data + 1


def add_one_hundred(data: pd.DataFrame):
    """
    Adds one to the current dataframe values.

    :param data:
    :return:
    """
    return data + 100


if __name__ == '__main__':
    data = pd.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])

    pl = Pipeline()
    pl.add_input(data)
    pl.add_transform(Transform(add_one))
    pl.add_transform(Transform(add_one_hundred))

    pl.add_check(Check(contains_no_duplicates, ['add_one']))  # True
    pl.add_check(Check(contains_no_nulls, ['add_one', 'add_one_hundred']))  # True
    pl.add_check(Check(contains_columns, ['add_one'], [['a', 'b']]))  # True
    pl.add_check(Check(is_less_than, ['add_one'], [100]))  # True
    # pl.add_check(Check(is_less_than, ['add_one_hundred'], [100]))  # False
    # pl.add_check(Check(contains_columns, ['add_one'], [['a', 'd']]))  # False

    print(pl)
    print(pl.steps)

    pl.run()
