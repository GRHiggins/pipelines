from typing import Callable


class Transform:
    def __init__(self, func: Callable, desc: str = None):
        self.func = func

        self._desc = desc

    @property
    def name(self):
        return self.func.__name__

    @property
    def desc(self):
        if self._desc is None:
            self._desc = self.func.__doc__.split('\n')[1].strip()
        return self._desc
