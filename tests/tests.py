from pathlib import Path

import pandas as pd
import pytest

from pipelines import Pipeline, Check, Transform, CheckFailed
from pipelines.utils import contains_columns, is_less_than


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


class TestPipelines:

    data = pd.read_json(Path(__file__).parent.joinpath('data.json'))

    def test_contains_columns(self):
        pl = Pipeline()
        pl.add_input(self.data.copy())
        pl.add_transform(Transform(add_one))
        pl.add_check(Check(contains_columns, ['add_one'], [['a', 'b']]))  # True
        pl.run()

    def test_less_than(self):
        pl = Pipeline()
        pl.add_input(self.data.copy())
        pl.add_transform(Transform(add_one))
        pl.add_check(Check(is_less_than, ['add_one'], [100]))  # True
        pl.run()

    def test_not_less_than(self):
        pl = Pipeline()
        pl.add_input(self.data.copy())
        pl.add_transform(Transform(add_one_hundred))
        pl.add_check(Check(is_less_than, ['add_one_hundred'], [100]))  # False
        with pytest.raises(CheckFailed):
            pl.run()


if __name__ == '__main__':
    pytest.main()
