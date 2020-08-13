import pandas as pd
df = pd.DataFrame({'id': ['spam', 'egg', 'egg', 'spam',
	'ham', 'ham'],
	'value1': [1, 5, 5, 2, 5, 5],
	'value2': list('abbaxy')})
lookuptable = None


df2 = pd.DataFrame({'id': ['spam', 'test', 'egg', 'spam',
	'ham', 'ham'],
	'value1': [2, 6, 5, 4, 7, 8],
	'value2': list('abbaxy')})

groupbyobject, hash_table, preuniques = df.pipeline_groupby('id')
ret1, lookuptable = groupbyobject.agg({'value1':'pipeline_nunique'}, lookuptable)
print(ret1)
print(table)
groupbyobject, hash_table, preuniques = df2.pipeline_groupby('id', hash_table=hash_table, pre_uniques=preuniques)
ret2, lookuptable2 = groupbyobject.agg({'value1':'pipeline_nunique'}, lookuptable)
print(ret2)
print(table2)
for index, row in ret2.iterrows():
	if index in ret1.index:
		ret1.loc[index,'value1']  += row['value1']
	else:
		ret1.loc[index, 'value1'] = row['value1']
print(ret1)

