import string
import timeit
import numpy as np

import logging
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from pandas import DataFrame, MultiIndex, Series, concat, date_range, merge, merge_asof
from pandas import pipeline_merge
import pandas.util.testing as tm

import argparse
try:
    from pandas import merge_ordered
except ImportError:
    from pandas import ordered_merge as merge_ordered

parser = argparse.ArgumentParser()
parser.add_argument('-left', default=50000, type=int)
parser.add_argument('-right', default=500000, type=int)
parser.add_argument('-chunk', default=20, type=int)
args = parser.parse_args()

left_table = args.left
right_table = args.right
N = 20000000
pieces = args.chunk
logger.info("Start generating data")
indices = tm.makeStringIndex(N).values
indices2 = tm.makeStringIndex(N).values
key = np.tile(indices[:left_table], 1)
left = DataFrame(
    {"key": key, "value": np.random.randn(left_table)}
)
right = {}
np.random.shuffle(indices)
for i in range(1, pieces):
    right[i] = DataFrame(
        {
            "key": indices2[(i - 1)*right_table + 5000:i*right_table + 5000],
            "value2": np.random.randn(right_table),
        }
    )
logger.info("Finish generating data")
logger.info("Left table size: " + str(left_table) + ", right table chunk size: " + str(right_table))
#right[12] = DataFrame(
#    {
#        "key": indices[(11)*1000000 + 50000:11*1000000 + 50000 + 600000],
#        "value2": np.random.randn(600000),
#    }
#)
print("\n")

logger.info("Start Running test for original pandas code")
prev = 0
for ttt in range(2, pieces + 1):
    right_merge = DataFrame(columns=["key", "value2"])
    for i in range(1, ttt):
        right_merge = right_merge.append(right[i])
    #print(right_merge)
    start = timeit.default_timer()
    result = merge(left, right_merge, how="inner")
    end = timeit.default_timer()
    logger.info(str(ttt - 1) + " chunks take time: " + str(end - start) + " single chunk takes time: " + str(end - start - prev))
    prev = end - start
    #print("******* ", end - start)

print("--------------------------------------------------------------------------------")
print("\n")


logger.info("Start Running test for original pandas code, in increment manner")
total = 0
for i in range(1, pieces):
    start = timeit.default_timer()
    result = merge(left, right[i], how="inner")
    end = timeit.default_timer()
    logger.info(str(i) + "th single chunk takes time: " + str(end - start))
    total += end - start
    #print("******* ", end - start)
logger.info("Original increment takes time: " + str(total))

print("--------------------------------------------------------------------------------")
print("\n")

logger.info("Start Running test for pipelined pandas code")

leftsorter = None
leftcount = None
orizer = None
intrizer = None
count = 0
for i in range(1, pieces):
    start = timeit.default_timer()
    result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, right[i], factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=ttt-1, how="pipeline")
    end = timeit.default_timer()
    count += (end - start)
    logger.info(str(i) + " chunks take time " +  str(end - start) + " Accum time: " + str(count))
   #print("******* ", end - start)


print("--------------------------------------------------------------------------------")
print("\n")
logger.info("Start Running test for pipelined pandas with merge join code")

leftsorter = None
leftcount = None
orizer = None
intrizer = None
count = 0
for i in range(1, pieces):
    start = timeit.default_timer()
    result, orizer, intrizer, leftsorter, leftcount = pipeline_merge(left, right[i], factorizer=orizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, slices=ttt-1, how="pipeline_merge")
    end = timeit.default_timer()
    count += (end - start)
    logger.info(str(i) + " chunks take time " +  str(end - start) + " Accum time: " + str(count))
   #print("******* ", end - start)

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
