import pandas as pd

#df = pd.DataFrame({'A': [1.1, 1.1, 2.1, 1.1, 2.1],
#                   'B': [3, 6, 3, 4, 5],
#                   'C': [10, 12, 14, 11, 13]}, columns=['A', 'B', 'C'])

df = pd.DataFrame({'A': [1.1, 2.1, 1.1, 2.1],
                   'B': [3, 6, 2, 4]}, columns=['A', 'B'])

df2 = pd.DataFrame({'A': [1.1, 2.1, 1.1, 2.2],
                   'B': [13, 12, 11, 10]}, columns=['A', 'B'])

#print(df.pipeline_groupby)
#print(df.groupby)
#print(df.groupby('A').sum)
groupbyobject, table, preuniques = df.pipeline_groupby('A')
print("&&&&&&&&&&&&&&&")
print(groupbyobject)
print("&&&&&&&&&&&&&&&")
(ret, inter), table, preuniques = groupbyobject.pipeline_sum()
groupbyobject, table, preuniques = df2.pipeline_groupby('A', hash_table=table, pre_uniques=preuniques)
(ret2, inter2), table, preuniques = groupbyobject.pipeline_sum(inter)
#print(ret)
#print(table)
#print(inter)
#
print(ret2)
#print(table)
#print(inter2)
#print(df.groupby('A').prod())
#print(df.groupby('A').min())
#print(df.groupby('A').max())
print("&***********")
#ret, table = df.groupby('A', pipeline=True, hash_table = None).mean()
#print("****")
#print(ret)
#print("****")
#print(table)
#print(pd.Grouper(key='A'))

