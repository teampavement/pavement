#!/usr/bin/env python3
import bisect, collections, operator, os, sys, pytz
from datetime import datetime, timedelta, timezone, time
from dateutil import parser
from pathlib import Path
from flask import Flask, json, jsonify, abort, request
from flask_cors import CORS
from api import tables
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '..'))
import config

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config.from_object(os.environ['APP_SETTINGS'])
tables.db.init_app(app)
ApTransactions = tables.ApTransactions

days = { # todo: centralize somewhere else
    'monday': 0,
    'tuesday': 1,
    'wednesday': 2,
    'thursday': 3,
    'friday': 4,
    'saturday': 5,
    'sunday': 6
}

midnight = time(4, 0, tzinfo=timezone.utc)
off_hours_start = time(6, 0, tzinfo=timezone.utc) # 2am
off_hours_end = time(13, 0, tzinfo=timezone.utc) # 9am
cached_time = 0
cached_interval = 0

@app.route('/parking-spaces', methods = ['GET'])
def get_parking_spaces():
    pass

@app.route('/parking-occupancy', methods = ['POST'])
def get_parking_occupancy():
    params = get_params()
    datetime_range = get_datetime_range(params)
    day = get_day()
    filters = get_filters(params, datetime_range, False, day)
    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.expiry_date
        ).filter(*filters).order_by(ApTransactions.purchased_date).all()
    transactions = get_squashed_transactions(spaces, datetime_range)

    if day is not False:
        return get_occupancy_by_day(transactions, datetime_range, params, day)

    heatmap = request.args.get('heatmap', default = False)
    if heatmap:
        return get_occupancy(transactions, datetime_range, params)
    else:
        time_intervals = get_time_intervals(datetime_range, True)
        return get_bucketed_occupancy(transactions, time_intervals, params)

@app.route('/parking-revenue', methods = ['POST'])
def get_parking_revenue():
    params = get_params()
    datetime_range = get_datetime_range(params)
    day = get_day()
    filters = get_filters(params, datetime_range, True, day)
    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.revenue
        ).filter(*filters).all()

    if day is not False:
        return get_revenue_by_day(spaces, datetime_range, day)

    sum = request.args.get('sum', default = False)
    if sum:
        return get_revenue(spaces, datetime_range)
    else:
        time_intervals = get_time_intervals(datetime_range)
        return get_bucketed_revenue(spaces, time_intervals)

@app.route('/parking-time', methods = ['POST'])
def get_parking_time():
    params = get_params()
    datetime_range = get_datetime_range(params)
    day = get_day()
    filters = get_filters(params, datetime_range, True, day)
    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.expiry_date
        ).filter(*filters).all()

    if day is not False:
        return get_times_by_day(spaces, datetime_range, day)

    heatmap = request.args.get('heatmap', default = False)
    if heatmap:
        return get_times(spaces, datetime_range, params)
    else:
        time_intervals = get_time_intervals(datetime_range)
        return get_bucketed_times(spaces, time_intervals)

def get_params():
    return {} if not request.data else request.get_json()

def get_datetime_range(params):
    if 'datetime_range' not in params:
        start_date = datetime(2016, 1, 1, tzinfo=timezone.utc) # can select MIN from db later (note tz)
        end_date = datetime(2019, 1, 1, tzinfo=timezone.utc) # can select MAX from db later (note tz)
    else:
        if 'start' not in params['datetime_range']:
            start_date = datetime(2016, 1, 1, tzinfo=timezone.utc)
        else:
            start_date = parser.parse(params['datetime_range']['start'])

        if 'end' not in params['datetime_range']:
            end_date = datetime(2019, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = parser.parse(params['datetime_range']['end'])

    return {'start' : start_date, 'end': end_date}

def get_squashed_transactions(spaces, datetime_range):
    # get all transactions and remove overlapping time ranges
    transactions = {}
    for id, start_time, end_time in spaces:
        if end_time is None:
            continue
        if id not in transactions:
            transactions[id] = [{'start': start_time, 'end': end_time}]
        else:
            if start_time < transactions[id][-1]['end']:
                start = transactions[id][-1]['end']
            else:
                start = start_time
            if end_time <= transactions[id][-1]['end']:
                continue

            transactions[id].append({'start': start, 'end': end_time})

    return transactions

def get_filters(params, datetime_range, only_check_start = False, day = False):
    if only_check_start:
        filters = [
            datetime_range['start'] <= ApTransactions.purchased_date,
            datetime_range['end'] > ApTransactions.purchased_date
        ]
    else:
        filters = [
            datetime_range['start'] < ApTransactions.expiry_date,
            datetime_range['end'] > ApTransactions.purchased_date
        ]

    if 'parking_spaces' in params:
        if is_curbs(params['parking_spaces']):
            spaces = []
            for curb in params['parking_spaces']:
                spaces += curb
        else:
            spaces = params['parking_spaces']

        filters.append(ApTransactions.stall.in_(spaces))

    if day is not False:
        filters.append(day == ApTransactions.parking_day)

    return filters

def get_time_intervals(datetime_range, include_end = False, time_delta = False, parking_day = False):
    if not time_delta:
        time_diff = (datetime_range['end'] - datetime_range['start']).total_seconds()
        time_delta = get_time_delta(time_diff)

    return generate_times(datetime_range, time_delta, include_end, parking_day)

def get_time_delta(time_diff):
    if time_diff <= 259200: # 3 days
        return timedelta(hours=1)
    elif time_diff > 259200 and time_diff <= 2678400: # 3-31 days
        return timedelta(days=1)
    else:
        return timedelta(weeks=1)

def generate_times(datetime_range, delta, include_end, parking_day):
    times = []
    curr = datetime_range['start']
    op = operator.le if include_end else operator.lt

    while op(curr, datetime_range['end']):
        if ((curr.timetz() >= off_hours_start and curr.timetz() < off_hours_end)
            or (parking_day is not False and not in_parking_day(curr, parking_day))):
            curr += delta
            continue

        times.append(curr)
        curr += delta

    if (include_end and times[-1] != datetime_range['end']):
        if parking_day is False:
            times.append(datetime_range['end'])
        else:
            times.append(times[-1] + delta)

    return times

def get_bucketed_spaces(spaces, times):
    bucketed_spaces = [set() for _ in times]

    for id, start_time, end_time in spaces:
        if end_time is None:
            continue

        for i in range(bisect.bisect_left(times, start_time) - 1, bisect.bisect_right(times, end_time)):
            if i < 0:
                continue
            bucketed_spaces[i].add(id)

    return bucketed_spaces

def get_occupancy_by_day(spaces, datetime_range, params, day):
    time_intervals = get_time_intervals(datetime_range, True, timedelta(hours=1), day)
    hour_occupancy = {}
    occupancy = get_bucketed_occupancy(spaces, time_intervals, params, True)
    days = 0
    curr_date = None

    for i, timestamp in enumerate(time_intervals):
        timestamp = timestamp.astimezone(pytz.timezone('America/New_York'))
        if timestamp.weekday() == day and curr_date != timestamp.date():
            days += 1

        curr_date = timestamp.date()
        hour = timestamp.timetz()
        if hour not in hour_occupancy:
            hour_occupancy[hour] = occupancy['bucketed'][i] / occupancy['potential'][i]
        else:
            hour_occupancy[hour] += occupancy['bucketed'][i] / occupancy['potential'][i]

    return jsonify(get_ordered_results(hour_occupancy, days))

def get_occupancy(spaces, datetime_range, params):
    occupancy = {'data': []}

    if 'parking_spaces' in params and is_curbs(params['parking_spaces']):
        for curb in params['parking_spaces']:
            curb_time = 0
            potential_curb_time = len(curb) \
                * get_potential_occupancy(datetime_range['start'], datetime_range['end'])

            for space in curb:
                space_time = 0

                if space in spaces:
                    for transaction in spaces[space]:
                        time_range = get_bound_time_range(transaction, datetime_range)
                        if not time_range:
                            continue
                        space_time += (time_range['end'] - time_range['start']).total_seconds()

                curb_time += space_time

            curb_occupancy = round(curb_time / potential_curb_time, 2)
            occupancy['data'].append({'value': curb_occupancy, 'space': curb})
    else:
        potential_time = get_potential_occupancy(datetime_range['start'], datetime_range['end'])

        for id, transactions in spaces.items():
            space_time = 0

            for transaction in transactions:
                time_range = get_bound_time_range(transaction, datetime_range)
                if not time_range:
                    continue
                space_time += (time_range['end'] - time_range['start']).total_seconds()

            space_time = round(space_time / potential_time, 2)
            occupancy['data'].append({'value': space_time, 'space': id})

        if 'parking_spaces' in params:
            for space in params['parking_spaces']:
                if space not in spaces:
                    occupancy['data'].append({'value': 0, 'space': space})

    return jsonify(occupancy)

def get_bucketed_occupancy(spaces, time_intervals, params, return_raw = False):
    bucketed_occupancy = [0] * (len(time_intervals) - 1)

    for id, transactions in spaces.items():
        data = get_time_data(
            transactions[0]['start'], # first transaction
            transactions[-1]['end'], # last transaction
            time_intervals
        )

        for i in range(data['start_index'], data['end_index']):
            for transaction in transactions:
                if (transaction['start'] > time_intervals[i+1]
                    or transaction['end'] < time_intervals[i]):
                    continue

                data = get_time_data(
                    transaction['start'],
                    transaction['end'],
                    time_intervals
                )

                if transaction['start'] < time_intervals[i]:
                    data['start_time'] = time_intervals[i]
                else:
                    data['start_time'] = transaction['start']
                if transaction['end'] > time_intervals[i+1]:
                    data['end_time'] = time_intervals[i+1]
                else:
                    data['end_time'] = transaction['end']

                bucketed_occupancy[i] += get_time_occupied(i, data, time_intervals)

    potential_occupancy = get_potential_occupancy_bucketed(params, time_intervals)
    del time_intervals[-1]

    if return_raw:
        return {
            'bucketed': bucketed_occupancy,
            'potential': potential_occupancy
        }

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': round(bucketed_occupancy[i] / potential_occupancy[i], 2),
            } for i, timestamp in enumerate(time_intervals)]
    })

def get_revenue_by_day(spaces, datetime_range, day):
    time_intervals = get_time_intervals(datetime_range, False, timedelta(hours=1), day)
    hour_revenue = {}
    bucketed_revenue = get_bucketed_revenue(spaces, time_intervals, True)
    days = 0
    curr_date = None

    for i, timestamp in enumerate(time_intervals):
        timestamp = timestamp.astimezone(pytz.timezone('America/New_York'))
        if timestamp.weekday() == day and curr_date != timestamp.date():
            days += 1

        curr_date = timestamp.date()
        hour = timestamp.timetz()
        if hour not in hour_revenue:
            hour_revenue[hour] = bucketed_revenue[i]
        else:
            hour_revenue[hour] += bucketed_revenue[i]

    return jsonify(get_ordered_results(hour_revenue, days))

def get_revenue(spaces, datetime_range):
    revenue_sum = {}

    for id, start_time, revenue in spaces:
        if (revenue is None
            or (start_time.timetz() >= off_hours_start
                and start_time.timetz() < off_hours_end)):
            continue

        if id not in revenue_sum:
            revenue_sum[id] = revenue
        else:
            revenue_sum[id] += revenue

    return jsonify(revenue_sum)

def get_bucketed_revenue(spaces, time_intervals, return_raw = False):
    bucketed_revenue = [0] * len(time_intervals)

    for id, start_time, revenue in spaces:
        if (revenue is None
            or (start_time.timetz() >= off_hours_start
                and start_time.timetz() < off_hours_end)):
            continue

        start_index = bisect.bisect_left(time_intervals, start_time) - 1
        if start_index < 0:
            continue
        bucketed_revenue[start_index] += revenue

    if return_raw:
        return bucketed_revenue

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': bucketed_revenue[i],
            } for i, timestamp in enumerate(time_intervals)]
    })

def get_times_by_day(spaces, datetime_range, day):
    time_intervals = get_time_intervals(datetime_range, False, timedelta(hours=1), day)
    hour_times = {}
    bucketed = get_bucketed_times(spaces, time_intervals, True)
    days = 0
    curr_date = None

    for i, timestamp in enumerate(time_intervals):
        timestamp = timestamp.astimezone(pytz.timezone('America/New_York'))
        if timestamp.weekday() == day and curr_date != timestamp.date():
            days += 1

        curr_date = timestamp.date()
        hour = timestamp.timetz()
        if hour not in hour_times:
            hour_times[hour] = seconds_to_hours(bucketed['times'][i] / bucketed['spaces'][i])
        else:
            hour_times[hour] += seconds_to_hours(bucketed['times'][i] / bucketed['spaces'][i])

    return jsonify(get_ordered_results(hour_times, days))

def get_times(spaces, datetime_range, params):
    times = {'data': []}

    if 'parking_spaces' in params and is_curbs(params['parking_spaces']):
        spaces = get_keyed_spaces(spaces)
        for curb in params['parking_spaces']:
            curb_time = 0
            unused_spaces = 0

            for space in curb:
                space_time = 0

                if space in spaces:
                    for transaction in spaces[space]:
                        space_time += (transaction['end'] - transaction['start']).total_seconds()

                    curb_time += space_time / len(spaces[space])
                else:
                    unused_spaces += 1

            curb_time = curb_time / (len(curb) - unused_spaces)
            times['data'].append({'value': seconds_to_hours(curb_time), 'space': curb})
    else:
        keyed_spaces = {}
        for id, start_time, end_time in spaces:
            if end_time is None:
                continue

            if id not in keyed_spaces:
                keyed_spaces[id] = [{'start': start_time, 'end': end_time}]
            else:
                keyed_spaces[id].append({'start': start_time, 'end': end_time})

        for id, transactions in keyed_spaces.items():
            space_time = 0
            for transaction in transactions:
                space_time += (transaction['end'] - transaction['start']).total_seconds()

            space_time = space_time / len(transactions)
            times['data'].append({'value': seconds_to_hours(space_time), 'space': id})

        if 'parking_spaces' in params:
            for space in params['parking_spaces']:
                if space not in keyed_spaces:
                    times['data'].append({'value': 0, 'space': space})

    return jsonify(times)

def get_bucketed_times(spaces, time_intervals, return_raw = False):
    bucketed_times = [0] * (len(time_intervals))
    bucketed_spaces = [0] * (len(time_intervals))

    for id, start_time, end_time in spaces:
        if (end_time is None
            or (start_time.timetz() >= off_hours_start
                and start_time.timetz() < off_hours_end)):
            continue

        start_index = bisect.bisect_left(time_intervals, start_time) - 1
        if start_index < 0:
            continue

        bucketed_times[start_index] += (end_time - start_time).total_seconds()
        bucketed_spaces[start_index] += 1

    for i, space in enumerate(bucketed_spaces):
        if space == 0:
            bucketed_spaces[i] += 1

    if return_raw:
        return {
            'times': bucketed_times,
            'spaces': bucketed_spaces
        }

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': seconds_to_hours(bucketed_times[i] / bucketed_spaces[i]),
        } for i, timestamp in enumerate(time_intervals)]
    })

def get_ordered_results(results, days):
    ordered_results = collections.OrderedDict(sorted(results.items()))
    results_by_day = {'data': []}
    for hour, value in ordered_results.items():
        avg_value = round(value / days, 2)
        first_hour = format_hour(hour)
        next_hour = format_hour(get_next_hour(hour))
        timestamp = first_hour + '-' + next_hour
        results_by_day['data'].append({'timestamp': timestamp, 'value': avg_value})

    return rearrange_times(results_by_day)

def get_time_data(start_time, end_time, times):
    start_index = bisect.bisect_left(times, start_time) - 1
    end_index = bisect.bisect_right(times, end_time)
    data = {
        'start_time': start_time, 'end_time': end_time,
        'start_index': start_index, 'end_index': end_index
    }

    if start_index < 0:
        data['start_index'] = 0
        data['start_time'] = times[data['start_index']]
    if end_index == len(times):
        data['end_index'] = end_index - 1
        data['end_time'] = times[data['end_index']]

    return data

def get_time_occupied(i, time_data, times):
    if time_data['start_index'] == (time_data['end_index'] - 1):
        return (time_data['end_time'] - time_data['start_time']).total_seconds()
    elif i == time_data['start_index']:
        return (times[i+1] - time_data['start_time']).total_seconds()
    elif i == (time_data['end_index'] - 1):
        return (time_data['end_time'] - times[i]).total_seconds()
    else:
        return (times[i+1] - times[i]).total_seconds()

def get_potential_occupancy_bucketed(params, time_intervals):
    potential_occupancy = [0] * (len(time_intervals) - 1)
    if 'parking_spaces' in params:
        space_count = len(params['parking_spaces'])
    else:
        # slowish query we can worry about optimizing later when we get more data
        space_count = 3693

    for i, interval in enumerate(time_intervals):
        if i == len(time_intervals) - 1:
            break

        potential_occupancy[i] = space_count \
            * get_potential_occupancy(interval, time_intervals[i+1])

    return potential_occupancy

def get_potential_occupancy(start_time, end_time):
    global cached_time
    global cached_interval

    if cached_interval > 0 and (end_time - start_time).total_seconds() == cached_interval:
        return cached_time

    seconds = 0
    delta = timedelta(seconds=1)
    curr = start_time

    while curr < end_time:
        curr += delta
        if curr.timetz() == off_hours_start:
            curr += timedelta(hours=7)
        seconds += 1

    cached_time = seconds
    cached_interval = (end_time - start_time).total_seconds()
    return seconds

def get_keyed_spaces(spaces):
    keyed_spaces = {}

    for id, start_time, end_time in spaces:
        if end_time is None:
            continue

        if id not in keyed_spaces:
            keyed_spaces[id] = [{'start': start_time, 'end': end_time}]
        else:
            keyed_spaces[id].append({'start': start_time, 'end': end_time})

    return keyed_spaces

def is_curbs(spaces):
    return isinstance(spaces[0], list)

def get_bound_time_range(transaction, datetime_range):
    if (transaction['start'] > datetime_range['end']
        or transaction['end'] < datetime_range['start']):
        # this could happen when receiving squashed transactions
        return False

    if transaction['start'] < datetime_range['start']:
        start_time = datetime_range['start']
    else:
        start_time = transaction['start']
    if transaction['end'] > datetime_range['end']:
        end_time = datetime_range['end']
    else:
        end_time = transaction['end']

    return {'start': start_time, 'end': end_time}

def seconds_to_hours(seconds):
    return round(seconds / 3600, 2)

def in_parking_day(dt, parking_day):
    day = dt.astimezone(pytz.timezone('America/New_York')).weekday()
    if dt.timetz() >= midnight and dt.timetz() < off_hours_start:
        day = 6 if day == 0 else day - 1

    return day == parking_day

def get_day():
    day = request.args.get('day', default = False)

    if day and day.lower() in days:
        return days[day.lower()]

    return False

def get_next_hour(hour):
    return (datetime(1, 1, 1, hour.hour, hour.minute, hour.second) \
            + timedelta(hours=1)).time()

def rearrange_times(result):
    data = result['data']

    if data[0]['timestamp'] == '12:00AM-1:00AM' and len(data) > 2:
        data = data[1:] + [data[0]]
    if data[0]['timestamp'] == '1:00AM-2:00AM' and len(data) > 2:
        data = data[1:] + [data[0]]

    result['data'] = data
    return result

def format_hour(hour):
    return hour.strftime('%I:%M%p').lstrip('0')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
