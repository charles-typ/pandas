import boto3
s3 = boto3.resource('s3')
s3_bucket =  "jiffy-error"
s3_key =  "pywren.runtime/pywren_runtime-3.6-default_pandas.meta.json"
obj = s3.Object(s3_bucket, s3_key)
body = obj.get()['Body'].read()
print(body)
