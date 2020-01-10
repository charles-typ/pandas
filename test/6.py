import string
import timeit
import numpy as np

from pandas import DataFrame, MultiIndex, Series, concat, date_range, merge, merge_asof
from pandas import pipeline_merge
import pandas.util.testing as tm

try:
    from pandas import merge_ordered
except ImportError:
    from pandas import ordered_merge as merge_ordered


N = 20000000
pieces = 10
indices = tm.makeStringIndex(N).values
key = np.tile(indices[:10000000], 1)
left = DataFrame(
    {"key": key, "value": np.random.randn(10000000)}
)
right = {}
np.random.shuffle(indices)
for i in range(1, pieces):
    right[i] = DataFrame(
        {
            "key": indices[(i - 1)*1000000 + 5000:i*1000000 + 5000],
            "value2": np.random.randn(1000000),
        }
    )
#right[12] = DataFrame(
#    {
#        "key": indices[(11)*1000000 + 50000:11*1000000 + 50000 + 600000],
#        "value2": np.random.randn(600000),
#    }
#
for ttt in range(2, pieces + 1):
    leftsorter = None
    leftcount = None
    orizer = None
    intrizer = None
    start = timeit.default_timer()
    for i in range(1, ttt):
        result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, right[i], factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=ttt-1, how="pipeline")
    end = timeit.default_timer()
    print("******* ", end - start)

print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

for ttt in range(2, pieces + 1):
    right_merge = DataFrame(columns=["key", "value2"])
    for i in range(1, ttt):
        #print(right[i])
        right_merge = right_merge.append(right[i])
    #print(right_merge)
    start = timeit.default_timer()
    result = merge(left, right_merge, how="inner")
    end = timeit.default_timer()
    print("******* ", end - start)

print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

for ttt in range(2, pieces + 1):
    leftsorter = None
    leftcount = None
    orizer = None
    intrizer = None
    start = timeit.default_timer()
    for i in range(1, ttt):
        result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, right[i], factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=ttt-1, how="pipeline")
    end = timeit.default_timer()
    print("******* ", end - start)

print("################################")
for ttt in range(2, pieces + 1):
    right_merge = DataFrame(columns=["key", "value2"])
    for i in range(1, ttt):
        #print(right[i])
        right_merge = right_merge.append(right[i])
    #print(right_merge)
    start = timeit.default_timer()
    result = merge(left, right_merge, how="inner")
    end = timeit.default_timer()
    print("******* ", end - start)

#for ttt in range(2, pieces + 1):
#    leftsorter = None
#    leftcount = None
#    orizer = None
#    intrizer = None
#    start = timeit.default_timer()
#    for i in range(1, ttt):
#        result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, right[i], factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, how="pipeline")
#    end = timeit.default_timer()
#    print(end - start)
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


#from .pandas_vb_common import setup  # noqa: F401 isort:skip
