#!/usr/bin/env python3
import bisect, operator, os, sys
import simplejson as json
from datetime import datetime, timedelta, timezone
from dateutil import parser
from pathlib import Path
from flask import Flask, json, jsonify, abort, request
from tables import db, ApTransactions
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '..'))
import config

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
db.init_app(app)

@app.route('/parking-spaces', methods = ['GET'])
def get_parking_spaces():
    pass

@app.route('/parking-occupancy', methods = ['POST'])
def get_parking_occupancy():
    params = get_params()
    datetime_range = get_datetime_range(params)
    filters = get_filters(params, datetime_range)
    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.expiry_date
        ).filter(*filters).all()
    transactions = get_squashed_transactions(spaces, datetime_range)
    time_intervals = get_time_intervals(datetime_range, True)

    heatmap = request.args.get('heatmap', default = False)
    if heatmap:
        return get_occupancy(transactions, datetime_range, params)
    else:
        return get_bucketed_occupancy(transactions, time_intervals, params)

@app.route('/parking-revenue', methods = ['POST'])
def get_parking_revenue():
    params = get_params()
    datetime_range = get_datetime_range(params)
    filters = [
        datetime_range['start'] <= ApTransactions.purchased_date,
        datetime_range['end'] > ApTransactions.purchased_date
    ]

    if 'parking_spaces' in params:
        filters.append(ApTransactions.stall.in_(params['parking_spaces']))

    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.revenue
        ).filter(*filters).all()
    time_intervals = get_time_intervals(datetime_range)
    bucketed_revenue = get_bucketed_revenue(spaces, time_intervals)

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': bucketed_revenue[i],
            } for i, timestamp in enumerate(time_intervals)]
    })

@app.route('/parking-time', methods = ['POST'])
def get_parking_time():
    params = get_params()
    datetime_range = get_datetime_range(params)
    filters = get_filters(params, datetime_range)
    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.expiry_date
        ).filter(*filters).all()

    heatmap = request.args.get('heatmap', default = False)
    if heatmap:
        return get_times(spaces, datetime_range, params)
    else:
        return get_bucketed_times(spaces, datetime_range)

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

def get_filters(params, datetime_range):
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

    return filters

def get_time_intervals(datetime_range, include_end = False):
    time_diff = (datetime_range['end'] - datetime_range['start']).total_seconds()
    if time_diff <= 259200: # 3 days
        return generate_times(datetime_range, timedelta(hours=1), include_end)
    elif time_diff > 259200 and time_diff <= 2678400: # 3-31 days
        return generate_times(datetime_range, timedelta(days=1), include_end)
    else:
        return generate_times(datetime_range, timedelta(weeks=1), include_end)

def generate_times(datetime_range, delta, include_end):
    times = []
    curr = datetime_range['start']
    op = operator.le if include_end else operator.lt
    while op(curr, datetime_range['end']):
        times.append(curr)
        curr += delta

    if (include_end and times[-1] != datetime_range['end']):
        times.append(datetime_range['end'])

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

def get_occupancy(spaces, datetime_range, params):
    occupancy = {'data': []}

    if 'parking_spaces' in params and is_curbs(params['parking_spaces']):
        for curb in params['parking_spaces']:
            curb_time = 0
            potential_curb_time = len(curb) \
                * (datetime_range['end'] - datetime_range['start']).total_seconds()

            for space in curb:
                space_time = 0

                if space in spaces:
                    for transaction in spaces[space]:
                        time_range = get_bound_time_range(transaction, datetime_range)
                        space_time += (time_range['end'] - time_range['start']).total_seconds()

                curb_time += space_time

            curb_occupancy = round(curb_time / potential_curb_time, 2)
            occupancy['data'].append({'value': curb_occupancy, 'space': curb})
    else:
        potential_time = (datetime_range['end'] - datetime_range['start']).total_seconds()

        for id, transactions in spaces.items():
            time = 0

            for transaction in transactions:
                time_range = get_bound_time_range(transaction, datetime_range)
                time += (time_range['end'] - time_range['start']).total_seconds()

            time = round(time / potential_time, 2)
            occupancy['data'].append({'value': time, 'space': id})

        if 'parking_spaces' in params:
            for space in params['parking_spaces']:
                if space not in spaces:
                    occupancy['data'].append({'value': 0, 'space': space})

    return jsonify(occupancy)

def get_bucketed_occupancy(spaces, time_intervals, params):
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

    potential_occupancy = get_potential_occupancy(params, time_intervals)
    del time_intervals[-1]

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': round(bucketed_occupancy[i] / potential_occupancy[i], 2),
            } for i, timestamp in enumerate(time_intervals)]
    })

def get_bucketed_revenue(spaces, times):
    bucketed_revenue = [0] * len(times)
    for id, start_time, revenue in spaces:
        if revenue is None:
            continue

        start_index = bisect.bisect_left(times, start_time) - 1
        bucketed_revenue[start_index] += revenue

    return bucketed_revenue

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
                        time = get_bound_time_range(transaction, datetime_range)
                        space_time += (time['end'] - time['start']).total_seconds()

                    curb_time += space_time / len(spaces[space])
                else:
                    unused_spaces += 1

            curb_time = round(curb_time / (len(curb) - unused_spaces), 2)
            times['data'].append({'value': curb_time, 'space': curb})
    else:
        keyed_spaces = {}
        for id, start_time, end_time in spaces:
            if end_time is None:
                continue

            transaction = {'start': start_time, 'end': end_time}
            time = get_bound_time_range(transaction, datetime_range)

            if id not in keyed_spaces:
                keyed_spaces[id] = [{'start': time['start'], 'end': time['end']}]
            else:
                keyed_spaces[id].append({'start': time['start'], 'end': time['end']})

        for id, transactions in keyed_spaces.items():
            time = 0
            for transaction in transactions:
                time += (transaction['end'] - transaction['start']).total_seconds()

            time = round(time / len(transactions), 2)
            times['data'].append({'value': time, 'space': id})

        if 'parking_spaces' in params:
            for space in params['parking_spaces']:
                if space not in keyed_spaces:
                    times['data'].append({'value': 0, 'space': space})

    return jsonify(times)

def get_bucketed_times(spaces, datetime_range):
    time_intervals = get_time_intervals(datetime_range, True)
    bucketed_times = [0] * (len(time_intervals) - 1)

    for id, start_time, end_time in spaces:
        if end_time is None:
            continue

        data = get_time_data(start_time, end_time, time_intervals)
        for i in range(data['start_index'], data['end_index']):
            bucketed_times[i] += get_time_occupied(i, data, time_intervals)

    del time_intervals[-1]

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': round(bucketed_times[i] / len(spaces), 2),
        } for i, timestamp in enumerate(time_intervals)]
    })

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

def get_potential_occupancy(params, times):
    potential_occupancy = [0] * (len(times) - 1)
    if 'parking_spaces' in params:
        space_count = len(params['parking_spaces'])
    else:
        space_count = len(ApTransactions.query.with_entities(ApTransactions.stall)
                          .distinct(ApTransactions.stall).all())
    for i, time in enumerate(times):
        if i == len(times) - 1:
            break
        potential_occupancy[i] = (times[i+1] - time).total_seconds() * space_count

    return potential_occupancy

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
    if transaction['start'] < datetime_range['start']:
        start_time = datetime_range['start']
    else:
        start_time = transaction['start']
    if transaction['end'] > datetime_range['end']:
        end_time = datetime_range['end']
    else:
        end_time = transaction['end']

    return {'start': start_time, 'end': end_time}
