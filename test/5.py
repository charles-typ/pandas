import string

import numpy as np

from pandas import DataFrame, MultiIndex, Series, concat, date_range, merge, merge_asof
from pandas import pipeline_merge
import pandas.util.testing as tm

try:
    from pandas import merge_ordered
except ImportError:
    from pandas import ordered_merge as merge_ordered



N = 10000
indices = tm.makeStringIndex(N).values
indices2 = tm.makeStringIndex(N).values
key = np.tile(indices[:8000], 10)
key2 = np.tile(indices2[:8000], 10)
left = DataFrame(
    {"key": key, "value": np.random.randn(80000)}
)
right = DataFrame(
    {
        "key": indices[2000:],
        "value2": np.random.randn(8000),
    }
)

df = DataFrame(
    {
        "key1": np.tile(np.arange(500).repeat(10), 2),
        "key2": np.tile(np.arange(250).repeat(10), 4),
        "value": np.random.randn(10000),
    }
)
df2 = DataFrame({"key1": np.arange(500), "value2": np.random.randn(500)})
df3 = df[:5000]

result = pipeline_merge(left, right, how="pipeline")
print(result)
