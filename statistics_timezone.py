from pymongo import MongoClient
import os

db_host = os.environ['db_host']
db_name = os.environ['db_name']
db_port = int(os.environ['db_port'])
db_user = os.environ['db_user']
db_pwd = os.environ['db_pwd']

client = MongoClient(host=db_host, port=db_port)
db = client.get_database(db_name)
db.authenticate(db_user.strip(), db_pwd.strip())


delivery_timezone = {}
count_week = {}

def find_delivery():
    colles_delivery = db.delivery.find({}, {'_id': 0, 'country': 1, 'timezone': 1})

    for delivery in colles_delivery:
        timezone = delivery['timezone']
        if timezone not in delivery_timezone:
            delivery_timezone[timezone] = []
        if delivery['country'] not in delivery_timezone[timezone]:
            delivery_timezone[timezone].append(delivery['country'])


def find_first_day_pay():
    colles_fdp = db.first_day_pay.find({}, {'_id': 0, 'country': 1, 'platform': 1, 'week': 1, 'pay_hpur': 1,
                                            'total_pay_count': 1, 'pay_hour': 1})
    for fdp in colles_fdp:
        country = fdp['country']
        week = fdp['week']
        pay_hour = int(fdp['pay_hour'])
        for key, values in delivery_timezone.items():
            if country in values:
                if key not in count_week:
                    count_week[key] = {}
                # if week not in count_week[key]:
                #     count_week[key][week] = {}
                if pay_hour not in count_week[key]:
                    count_week[key][pay_hour] = 0

                count_week[key][pay_hour] += int(fdp['total_pay_count'])

    with open('count_week.txt', 'w') as fopen:
        title = 'timezone'
        for index in range(24):
            title += ',pay_hour'+str(index)
        fopen.write(title+'\n')
        for k0, v0 in count_week.items():
            s = k0
            for index in range(24):
                if index not in v0:
                    s = s+',0'
                else:
                    s = s+','+str(v0[index])
            fopen.write(s+'\n')
            print(s)


find_delivery()
find_first_day_pay()


