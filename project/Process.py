import random

f = open('Data.txt', 'r')
train1 = open('train1.txt', 'w')
train2 = open('train2.txt', 'w')
train3 = open('train3.txt', 'w')
train4 = open('train4.txt', 'w')
train5 = open('train5.txt', 'w')
validate = open('validate.txt', 'w')
test = open('test.txt', 'w')

def shuffle(filename):
    with open(filename+".txt", 'r') as source:
        data = [(random.random(), line) for line in source]
    data.sort()
    with open(filename + 'Shuffle.txt', 'w') as target:
        for _, line in data:
            target.write(line)

t1 = 242.5
t2 = 242.95
t3 = 242.95
t4 = 242.96
t5 = 242.96
t6 = 242.96
t7 = 242.96
t8 = 242.96
t9 = 242.96
t10 = 243.0
t11 = 245.0
t12 = 245.0
t13 = 245.0
t14 = 245.0
t15 = 245.0
t16 = 245.0
t17 = 245.0
t18 = 245.0

pop = 7.8276781566214355
date = '2015-10-08-13:57'

for line in f:
    label = 0
    classes = random.choices(population=[1, 2, 3, 4, 5, 6, 7], weights=[0.16, 0.16, 0.16, 0.16, 0.16, 0.1, 0.1])[0]
    arr = line.split(',')
    if float(arr[1]) >= t18:
        label = 1
    if classes == 1:
        train1.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")
    if classes == 2:
        train2.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")
    if classes == 3:
        train3.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")
    if classes == 4:
        train4.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")
    if classes == 5:
        train5.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")
    if classes == 6:
        validate.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")
    if classes == 7:
        test.write(
            "(" + str(label) + "," + date + "," + str(t1) + "," + str(t2) + "," + str(t3) + "," + str(t4) + "," + str(
                t5) + "," + str(t6) + "," + str(t7) + "," + str(t8) + "," + str(t9) + "," + str(t10) + "," + str(
                t11) + "," + str(t12) + "," + str(t13) + "," + str(t14) + "," + str(t15) + "," + str(t16) + "," + str(
                t17) + "," + str(t18) + "," + str(pop) + ")" + "\n")

    date = arr[0]
    t1 = t2
    t2 = t3
    t3 = t4
    t4 = t5
    t5 = t6
    t6 = t7
    t7 = t8
    t8 = t9
    t9 = t10
    t10 = t11
    t11 = t12
    t12 = t13
    t13 = t14
    t14 = t15
    t15 = t16
    t16 = t17
    t17 = t18
    t18 = float(arr[1])
    pop = float(arr[2])

shuffle('train1')
shuffle('train2')
shuffle('train3')
shuffle('train4')
shuffle('train5')
shuffle('validate')
shuffle('test')

