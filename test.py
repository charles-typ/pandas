import pandas as pd

df = pd.DataFrame({'A': [1.1, 1.1, 2.1, 1.1, 2.1],
                   'B': [3, 6, 3, 4, 5],
                   'C': [10, 12, 14, 11, 13]}, columns=['A', 'B', 'C'])

#print(df.pipeline_groupby)
#print(df.groupby)
#print(df.groupby('A').sum)
ret, table = df.pipeline_groupby('A')[0].pipeline_sum()
print(ret)
print(table)
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

