import pandas as pd

df = pd.DataFrame({'A': [1.1, 1.1, 2.1, 1.1, 2.1],
                   'B': [3, 6, 3, 4, 5],
                   'C': [10, 12, 14, 11, 13]}, columns=['A', 'B', 'C'])

print(df.groupby)
df.groupby('A', pipeline=False).mean()
#print(pd.Grouper(key='A'))

