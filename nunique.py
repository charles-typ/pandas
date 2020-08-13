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

ret1, table = df.groupby('id').agg({'value1':'pipeline_nunique'}, lookuptable)
print(ret1)
print(table)
ret2, table2 = df2.groupby('id').agg({'value1':'pipeline_nunique'}, table)
print(ret2)
print(table2)
for index, row in ret2.iterrows():
	if index in ret1.index:
		ret1.loc[index,'value1']  += row['value1']
	else:
		ret1.loc[index, 'value1'] = row['value1']
print(ret1)

