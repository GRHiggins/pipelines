from typing import Optional, List

import pandas as pd

from pipelines.checks import Check
from pipelines.exceptions import CheckFailed
from pipelines.transforms import Transform


class Pipeline:
    def __init__(self):
        self._data: Optional[pd.DataFrame] = None
        self._transforms: List[Transform] = []
        self._descriptions: List[str] = []
        self._checks: List[Check] = []

    def __repr__(self):
        return f'Pipeline({" -> ".join([transform.name for transform in self._transforms])})'

    @property
    def steps(self) -> str:
        return '\n'.join([f'- {transform.name}: {transform.desc}' for transform in self._transforms])

    def _check(self, transform: Transform, transformed: pd.DataFrame) -> None:
        checks = list(filter(lambda check: transform.name in check.steps, self._checks))
        for check in checks:
            if not check.run(transformed):
                raise CheckFailed(
                    f'Check "{check.name}" with params {check.params} failed after transform "{transform.name}"'
                )

    def add_input(self, data: pd.DataFrame) -> None:
        assert isinstance(data, pd.DataFrame)
        self._data = data.copy()

    def add_transform(self, transform: Transform) -> None:
        assert isinstance(transform, Transform)
        self._transforms.append(transform)

    def add_check(self, check: Check) -> None:
        assert isinstance(check, Check)
        self._checks.append(check)

    def run(self) -> pd.DataFrame:
        assert self._data is not None, 'Cannot run pipeline without inputs'
        transformed = self._data.copy()
        for transform in self._transforms:
            transformed = transform.func(transformed)
            self._check(transform, transformed)
        return transformed
