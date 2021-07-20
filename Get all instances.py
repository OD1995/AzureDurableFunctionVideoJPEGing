import requests
from collections import Counter
import json
from datetime import datetime

base_url = "https://futuresvideojpeging.azurewebsites.net/runtime/webhooks/durabletask/instances"
_code_ = '0HQCgKrzaol22Dawdyxz10UlhIrBQ1sM/yzTRssHrO6hFc03hWRmpA=='
## yyyy-MM-ddTHH:mm:ssZ
time_from = '2021-06-29T00:00:00Z'
time_to = '2021-06-30T23:00:00Z'


r = requests.get(
    url=base_url,
    params={
        'taskHub' : 'TaskHubOD',
        'code' : _code_,
        'createdTimeFrom' : time_from,
        'createdTimeTo' : time_to
        }
)
C = Counter([x['runtimeStatus'] for x in r.json()])
print(C)
#I = 0
#
#done_already = []
#for D in r.json():
#    startDT = datetime.strptime(
#        D['createdTime'],
#        "%Y-%m-%dT%H:%M:%SZ"
#    )
#    js = json.loads(D['input'])
#    if (startDT < datetime(year=2021,month=6,day=29)) & (js not in done_already):
#        r3 = requests.get(
#                'https://futuresvideojpeging.azurewebsites.net/api/HttpTrigger',
#                params=js
#            )
#        assert r3.ok
#        done_already.append(js)








