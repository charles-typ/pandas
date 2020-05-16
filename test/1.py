import pandas as pd

left = pd.DataFrame({'key': ['K0', 'K1', 'K2', 'K3', 'K2', 'K3', 'K0'],
                     'A': ['A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6'],
                     'B': ['B0', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6']})
left_2 = pd.DataFrame({'key': ['K0', 'K1', 'K2', 'K3', 'K4', 'K5'],
                      'A': ['AA0', 'AA1', 'AA2', 'AA3', 'AA4', 'AA5'],
                      'B': ['BB0', 'BB1', 'BB2', 'BB3', 'BB4', 'BB5']})

right = pd.DataFrame({'key': ['K0', 'K1', 'K2', 'K3', 'K5', 'K7'],
                      'C': ['C0', 'C1', 'C2', 'C3', 'C4', 'C5'],
                      'D': ['D0', 'D1', 'D2', 'D3', 'D4', 'D5']})

right_2 = pd.DataFrame({'key': ['K5', 'K4', 'K3', 'K5', 'K6', 'K9'],
                      'C': ['CC0', 'CC1', 'CC2', 'CC3', 'CC4', 'CC5'],
                      'D': ['DD0', 'DD1', 'DD2', 'DD3', 'DD4', 'DD5']})
left_full = pd.concat([left, left_2]) 
right_full = pd.concat([right, right_2])
result, orizer, intrizer, sorter, count = pd.pipeline_merge(left_full, right_full, slices=1, how="pipeline")
print(result)

keys, fac, intfac = pd.build_hash_table(left['key'])
keys, fac, intfac = pd.build_hash_table(left_2['key'], factorizer=fac, intfactorizer=intfac, previous_keys = keys)
print(left_full)
print(keys)

result, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(left_full, right,  factorizer=fac, intfactorizer=intfac, slices=2, how="pipeline",left_factorized_keys=keys)
print(result)
result, fac, intfac, leftsorter, leftcount = pd.pipeline_merge(left_full, right_2, factorizer=fac, intfactorizer=intfac, leftsorter=leftsorter, leftcount=leftcount, slices=2, how="pipeline",left_factorized_keys=keys)
print(result)
