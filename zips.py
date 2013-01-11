import csv
import pickle

store = {}
csv.register_dialect('zips', delimiter=',', quotechar='"',  skipinitialspace=True)

with open('zips.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"', dialect = 'zips')
    for i in reader:
        if i[5] == 'Virginia':
            store[i[0]] = (float(i[2]), float(i[3]))


output = open('zips.pkl', 'wb')
pickle.dump(store, output)
