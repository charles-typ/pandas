import pandas as pd

left = pd.DataFrame({'key': ['K0', 'K1', 'K2', 'K3'],
                     'A': ['A0', 'A1', 'A2', 'A3'],
                     'B': ['B0', 'B1', 'B2', 'B3']})
right = pd.DataFrame({'key': ['K0', 'K4', 'K5', 'K3'],
                      'C': ['C0', 'C1', 'C2', 'C3'],
                      'D': ['D0', 'D1', 'D2', 'D3']})
right2 = pd.DataFrame({'key': ['K0', 'K6', 'K7', 'K3'],
                      'C': ['C9', 'C1', 'C2', 'C8'],
                      'D': ['D9', 'D1', 'D2', 'D8']})
result, objectrizer, intrizer, leftsorter, leftcount = pd.pipeline_merge(left, right, how='pipeline')
print(result)
result, objectrizer, intrizer, leftsorter, leftcount = pd.pipeline_merge(left, right2, factorizer=objectrizer, intfactorizer=intrizer, leftsorter=leftsorter, leftcount=leftcount, how='pipeline')
print(result)
