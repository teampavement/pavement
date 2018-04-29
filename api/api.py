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
    spaces = get_space_time_ranges(params, datetime_range)
    time_intervals = get_time_intervals(datetime_range, True)
    bucketed_occupancy = get_bucketed_occupancy(spaces, time_intervals)
    potential_occupancy = get_potential_occupancy(params, time_intervals)
    del time_intervals[-1]

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': round(bucketed_occupancy[i] / potential_occupancy[i], 2),
            } for i, timestamp in enumerate(time_intervals)]
    })

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
    time_intervals = get_time_intervals(datetime_range, True)
    bucketed_times = get_bucketed_times(spaces, time_intervals)
    del time_intervals[-1]

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': bucketed_times[i],
            } for i, timestamp in enumerate(time_intervals)]
    })

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

def get_space_time_ranges(params, datetime_range):
    filters = get_filters(params, datetime_range)
    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.expiry_date
        ).filter(*filters).order_by(ApTransactions.purchased_date).all()

    space_time_ranges = {}
    for id, start_time, end_time in spaces:
        if end_time is None:
            continue
        if id not in space_time_ranges:
            space_time_ranges[id] = [{'start': start_time, 'end': end_time}]
        else:
            if start_time < space_time_ranges[id][-1]['end']:
                start = space_time_ranges[id][-1]['end']
            else:
                start = start_time
            if end_time <= space_time_ranges[id][-1]['end']:
                continue

            space_time_ranges[id].append({'start': start, 'end': end_time})

    return space_time_ranges

def get_filters(params, datetime_range):
    filters = [
        datetime_range['start'] < ApTransactions.expiry_date,
        datetime_range['end'] > ApTransactions.purchased_date
    ]

    if 'parking_spaces' in params:
        filters.append(ApTransactions.stall.in_(params['parking_spaces']))

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

def get_bucketed_occupancy(spaces, time_intervals):
    bucketed_occupancy = [0] * (len(time_intervals) - 1)

    for id, transactions in spaces.items():
        data = get_time_data(
            transactions[0]['start'], # first transaction
            transactions[-1]['end'], # last transaction
            time_intervals
        )

        start_time = data['start_time']
        end_time = data['end_time']

        for i in range(data['start_index'], data['end_index']):
            for transaction in transactions:
                if transaction['start'] < time_intervals[i]:
                    data['start_time'] = time_intervals[i]
                else:
                    data['start_time'] = transaction['start']
                if transaction['end'] > time_intervals[i+1]:
                    data['end_time'] = time_intervals[i+1]
                else:
                    data['end_time'] = transaction['end']

                bucketed_occupancy[i] += get_time_occupied(i, data, time_intervals)

    return bucketed_occupancy

def get_bucketed_revenue(spaces, times):
    bucketed_revenue = [0] * len(times)
    for id, start_time, revenue in spaces:
        if revenue is None:
            continue

        start_index = bisect.bisect_left(times, start_time) - 1
        bucketed_revenue[start_index] += revenue

    return bucketed_revenue

def get_bucketed_times(spaces, times):
    bucketed_times = [0] * (len(times) - 1)

    for id, start_time, end_time in spaces:
        if end_time is None:
            continue

        data = get_time_data(start_time, end_time, times)
        for i in range(data['start_index'], data['end_index']):
            bucketed_times[i] += get_time_occupied(i, data, times)

    return bucketed_times

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
