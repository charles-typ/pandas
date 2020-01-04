import string

import numpy as np

from pandas import DataFrame, MultiIndex, Series, concat, date_range, merge, merge_asof
import pandas.util.testing as tm

try:
    from pandas import merge_ordered
except ImportError:
    from pandas import ordered_merge as merge_ordered


class Merge:

#    params = [True, False]
#    param_names = ["sort"]
    timeout = 120.0
    def setup(self):
        N = 10000000
        indices = tm.makeStringIndex(N).values
        indices2 = tm.makeStringIndex(N).values
        key = np.tile(indices[:10000000], 1)
        key2 = np.tile(indices2[:500000], 1)
        self.left = DataFrame(
            {"key": key, "value": np.random.randn(10000000)}
        )
        self.right = DataFrame(
            {
                "key": indices[1000000:9000000],
                "value2": np.random.randn(8000000),
            }
        )

      #  self.df = DataFrame(
      #      {
      #          "key1": np.tile(np.arange(500).repeat(10), 1 ),
      #          "value": np.random.randn(5000),
      #      }
      #  )
      #  self.df2 = DataFrame({"key1": np.arange(500), "value2": np.random.randn(500)})
      #  self.df3 = self.df[:5000]

    def time_merge_2intkey(self):
        merge(self.left, self.right, how="inner")

#    def time_merge_dataframe_integer_2key(self, sort):
#        pipeline_merge(self.df, self.df3, how="pipeline")
#
#    def time_merge_dataframe_integer_key(self, sort):
#        pipeline_merge(self.df, self.df2, on="key1", how="pipeline")


# class I8Merge:
#
#     params = ["inner", "outer", "left", "right"]
#     param_names = ["how"]
#
#     def setup(self, how):
#         low, high, n = -1000, 1000, 10 ** 6
#         self.left = DataFrame(
#             np.random.randint(low, high, (n, 7)), columns=list("ABCDEFG")
#         )
#         self.left["left"] = self.left.sum(axis=1)
#         self.right = self.left.sample(frac=1).rename({"left": "right"}, axis=1)
#         self.right = self.right.reset_index(drop=True)
#         self.right["right"] *= -1
#
#     def time_i8merge(self, how):
#         merge(self.left, self.right, how=how)


from .pandas_vb_common import setup  # noqa: F401 isort:skip
