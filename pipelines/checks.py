from typing import Callable, List

import pandas as pd


class Check:
    def __init__(self, func: Callable, steps: List[str] = None, params: list = None):
        self.func = func
        self.steps = steps
        self.params = params

    @property
    def name(self):
        return self.func.__name__

    def run(self, data: pd.DataFrame):
        params = self.params if self.params is not None else []
        return self.func(data, *params)
