from psaw import PushshiftAPI
import time
from datetime import datetime, timedelta
api = PushshiftAPI()

def count(aft,bef):
    data = api.search_comments(q='Bitcoin', after=aft, before=bef)
    results = list(data)
    return len(results)

TWODAY = 86400 * 2

HOUR = 8 # suitable in China

start = 1445126400 # 2015.10.8

end = 1617926400 # 2021.4.9

f = open("CountEveryTwodDay.txt", "w")

while(start <= end) :
    dt = (datetime.fromtimestamp(start) - timedelta(hours=HOUR)).strftime('%Y-%m-%d %H:%M:%S')
    res = count(start - TWODAY, start)
    f.write(dt + "," + str(res)+"\n")
    start += TWODAY
    print(dt)

f.close()