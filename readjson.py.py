import json

with open('data.json') as data_file:
    data1 = json.load(data_file)


user = data1[0]['username']
pwd =  data1[0]['password']

print(user)
print(pwd)

