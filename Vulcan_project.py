import pandas as pd
import requests
import json

header = "Aa123456!"
url_s = 'http://interview.vulcancyber.com:3000/servers'
res_s = requests.get(url_s, headers={'Authorization': f'{header}'})
json_s = json.loads(res_s.content)
df_s = pd.DataFrame(json_s)



body = {"startId": 1, "amount": 50}
url_v = 'http://interview.vulcancyber.com:3000/vulns'
res_v = requests.post(url_v, data=json.dumps(body), headers={'Authorization': f'{header}'})
json_v = json.loads(res_v.content)
df_v = pd.DataFrame(json_v)


df_s['affects'] = df_s['os'] + '_' + df_s['osVersion']
df_s.drop(['os', 'osVersion'], inplace=True, axis=1)
df_f = pd.merge(df_s, df_v, how='left', left_on='affects', right_on='affects')
# df_f = df_f.dropna()
print (df_f)

