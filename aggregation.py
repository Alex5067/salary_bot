from datetime import datetime
from pymongo import MongoClient
import json

db = MongoClient('localhost', 27017)['salary_base']
sample_collection = db.salary

def aggregate_data(input_data):
    grouping_types = {
        'month': {"date": {"$dateTrunc": {"date": "$dt", "unit": "month"}}},
        'week': {"date": {"$dateTrunc": {"date": "$dt", "unit": "week"}}},
        'day': {"date": {"$dateTrunc": {"date": "$dt", "unit": "day"}}},
        'hour': {"date": {"$dateTrunc": {"date": "$dt", "unit": "hour"}}},
    }
    grouping_type = grouping_types.get(input_data['group_type'])

    if grouping_type is None:
        grouping_type = None

    aggregation = sample_collection.aggregate(
        [
            {"$match":
                {"dt": {
                    '$gte': datetime.strptime(input_data['dt_from'], "%Y-%m-%dT%H:%M:%S"),
                    '$lte': datetime.strptime(input_data['dt_upto'], "%Y-%m-%dT%H:%M:%S")
                }
                }
            },
            {"$densify": {
                "field": "dt",
                "range": {
                    "step": 1,
                    "unit": input_data['group_type'],
                    "bounds": [
                        datetime.strptime(input_data['dt_from'], "%Y-%m-%dT%H:%M:%S"),
                        datetime.strptime(input_data['dt_upto'], "%Y-%m-%dT%H:%M:%S")
                    ]
                }}
            },
            {"$project":
                 {"date_grouping": grouping_type, "value": "$value"}
             },
            {"$group":
                 {"_id": "$date_grouping", "total_value": {"$sum": "$value"}}
             },
            {"$sort":
                 {"_id": 1}
             }
        ]
    )

    labels = []
    data = []

    for item in aggregation:
        labels.append(datetime.strftime(item['_id']['date'], "%Y-%m-%dT%H:%M:%S"))
        data.append(item['total_value'])

    if "00:00:00" in input_data['dt_upto'] and (input_data['group_type'] == 'day' or input_data['group_type'] == 'hour'):
        labels.append(input_data['dt_upto'])
        data.append(0)

    result = {"dataset": data, "labels": labels}
    return json.dumps(result)


if __name__ == '__main__':
    # Пример использования
    input_data = {
    "dt_from": "2022-09-01T00:00:00",
    "dt_upto": "2022-12-31T23:59:00",
    "group_type": "month"
    }
    input_data2 = {
    "dt_from": "2022-10-01T00:00:00",
    "dt_upto": "2022-11-30T23:59:00",
    "group_type": "day"
    }
    input_data3 = {
    "dt_from": "2022-02-01T00:00:00",
    "dt_upto": "2022-02-02T00:00:00",
    "group_type": "hour"
    }
    result = aggregate_data(input_data2)
    print(result)



