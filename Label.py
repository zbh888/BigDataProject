import datetime
import math

TWODAY = 86400 * 2

def convert(date):
    date = datetime.datetime.strptime(date, "%Y-%m-%d-%H:%M") # For CountEveryTwodDay.txt
   # date = datetime.datetime.strptime(date, "%Y-%m-%d") # without Minute
    date = datetime.datetime.timestamp(date) + 8 * 3600 # China
    return int(date)

f = open("CountEveryTwoDayUnix.txt", "r")
container = []
for line in f:
    arr = line.split(",")
    container.append((int(arr[0]), int(arr[1])))

f.close()
w = open("LabelPoP.txt", "w")
f = open("data.txt", "r")
for line in f:
    arr = line.split(",")
    unix = convert(arr[0])
    i = 0
    result = 0.0
    while i < len(container):
        if container[i][0] <= unix < container[i + 1][0]:
            extra = unix-container[i][0]
            remain = TWODAY - extra
            result = math.log(extra/TWODAY * container[i+1][1] + remain/TWODAY * container[i][1])
            break
        i += 1
    w.write(line[:-1] + "," + str(result) + "\n")
