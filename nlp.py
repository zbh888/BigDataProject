from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from psaw import PushshiftAPI
from datetime import datetime, timedelta
nltk.download('vader_lexicon')


def nltkSentiment(view):
    sid = SentimentIntensityAnalyzer()
    total_pos = 0
    total_neg = 0
    total_neu = 0
    count  = len(view)
    for sen in view:
        senti = sid.polarity_scores(sen[0])
        neg = senti['neg']
        pos = senti['pos']
        if neg <= 0.1 and pos <= 0.1:
            total_neu += 1
        elif pos > neg:
            total_pos += 1
        else:
            total_neg += 1
    total = total_neg + total_neu + total_neg
    if total == 0:
        return -1, -1
    return total_pos / total, total_neg / total, count

api = PushshiftAPI()

def AnalyzeAndCount(dt,aft,bef,w):
    data = list(api.search_comments(q='bitcoin', after=aft, before=bef, filter=['body']))
    res = nltkSentiment(data)
    string = dt + "," + str(res[0]) + "," + str(res[1]) + "," + str(res[2]) + "\n"
    w.write(string)
    print(string)

TWODAY = 86400 * 2

start = 1444346095 # 2015.10.8

end = 1617923695 #2021.4.9

w = open("res.txt", "w")

while(start < end) :
    dt = str(start)
    AnalyzeAndCount(dt, start - TWODAY, start, w)
    start += TWODAY