from six.moves import cPickle as pickle
import pandas as pd


# stage_info_filename = 'ax.pickle'
# pickle.dump(chunks[0], open(stage_info_filename, 'wb'))
# info = pickle.load(open(stage_info_filename, "rb"))
# print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
# print(info)
# pickle.dump(chunks[1], open(stage_info_filename, 'wb'))
# info = pickle.load(open(stage_info_filename, "rb"))
# print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
# print(info)
query_name = "1"
appName = 'test-1'

debug_filename = "debug" + appName +  query_name + ".pickle"
info = pickle.load(open(debug_filename, "rb"))

print(pd.__version__)


aggregated = info.groupby(['sr_customer_sk', 'sr_store_sk']).agg({'sr_return_amt':'sum'}).reset_index()

aggregated.rename(columns = {'sr_store_sk':'ctr_store_sk',
    'sr_customer_sk':'ctr_customer_sk',
    'sr_return_amt':'ctr_total_return'
    }, inplace = True)  
print(aggregated)
