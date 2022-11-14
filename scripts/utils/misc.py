from collections import defaultdict
import json
import datetime

def merge_dict(d1, d2):
    dd = defaultdict(list)

    for d in (d1, d2):
        for key, value in d.items():
            if isinstance(value, list):
                dd[key].extend(value)
            else:
                dd[key].append(value)
    return dict(dd)

def merge_dict_string(dict_list):
    result = '['
    for idx, dict in enumerate(dict_list):
        if (idx != len(dict_list) - 1):
            result += json.dumps(dict, indent=4)
            result += ',\n'
        else:
            result += json.dumps(dict, indent=4)
            result += ']'
    return result

def get_time_export_string():
    now = datetime.datetime.now()
    now_formatted = now.strftime("%d-%m-%Y")
    return now_formatted

