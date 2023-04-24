# Pipelines
 Support for advanced data transformations in Python.

Pipelines provides a way to run complex sets of operations on Pandas Dataframes whilst checking data validity and reducing data
snooping.

Simple string representations of pipelines enhance code readability, thereby simplifying developer collaboration.

## Installation
```
pip install pipelines
```

## Usage
```
import pandas as pd

data = pd.DataFrame(
    [{"a": 1, "b": 2, "c": 3}, 
    {"a": 4, "b": 5, "c": 3}]
)
```

```
def add_one(data: pd.DataFrame):
    """
    Adds one to the current dataframe values.

    :param data: foo
    :return: bar
    """
    return data + 1
    
def add_one_hundred(data: pd.DataFrame):
    """
    Adds one to the current dataframe values.

    :param data:
    :return:
    """
    return data + 100
```

```
from pipelines import Pipeline
from pipelines.transforms import Transform

pl = Pipeline()
pl.add_input(data)
pl.add_transform(Transform(add_one))
pl.add_transform(Transform(add_one_hundred))
pl.run()
```

```
print(pl)
print(pl.steps)
```
Output:

Pipeline(add_one -> add_one_hundred)
- add_one: Adds one to the current dataframe values.
- add_one_hundred: Adds one to the current dataframe values.