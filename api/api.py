#!/usr/bin/env python3
import bisect, operator, pytz
import simplejson as json
from datetime import datetime, timedelta
from dateutil import parser, tz
from pathlib import Path
from flask import Flask, json, jsonify, abort, request
from tables import db, ApTransactions

app = Flask(__name__)
app.config.from_pyfile('../flask_config.py') # TODO: load this from main config.py object instead
db.init_app(app)
local_tz = pytz.timezone('US/Eastern')

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
    time_intervals = get_time_intervals(datetime_range)
    bucketed_spaces = get_bucketed_spaces(spaces, time_intervals)

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': len(bucketed_spaces[i]),
            'parking_spaces': [s for s in bucketed_spaces[i]]
            } for i, timestamp in enumerate(time_intervals)]
    })

@app.route('/parking-revenue', methods = ['POST'])
def get_parking_revenue():
    params = get_params()
    datetime_range = get_datetime_range(params)
    filters = [
        utc_to_local_stringify(datetime_range['start']) <= ApTransactions.purchased_date,
        utc_to_local_stringify(datetime_range['end']) > ApTransactions.purchased_date
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
    params = request.get_json()
    if params is None:
        params = {}

    return params

def get_datetime_range(params):
    if 'datetime_range' not in params:
        start_date = datetime(2016, 1, 1) # can select MIN from db later (note tz)
        end_date = datetime(2019, 1, 1) # can select MAX from db later (note tz)
    else:
        if 'start' not in params['datetime_range']:
            start_date = datetime(2016, 1, 1)
        else:
            start_date = parser.parse(params['datetime_range']['start'])

        if 'end' not in params['datetime_range']:
            end_date = datetime(2019, 1, 1)
        else:
            end_date = parser.parse(params['datetime_range']['end'])

    return  {'start' : start_date, 'end': end_date}

def get_filters(params, datetime_range):
    filters = [
        utc_to_local_stringify(datetime_range['start']) < ApTransactions.expiry_date,
        utc_to_local_stringify(datetime_range['end']) > ApTransactions.purchased_date
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

        start_time = local_to_utc(start_time)
        end_time = local_to_utc(end_time)

        for i in range(bisect.bisect_left(times, start_time) - 1, bisect.bisect_right(times, end_time)):
            if i < 0:
                continue
            bucketed_spaces[i].add(id)

    return bucketed_spaces

def get_bucketed_revenue(spaces, times):
    bucketed_revenue = [0] * len(times)
    for id, start_time, revenue in spaces:
        if revenue is None:
            continue

        start_time = local_to_utc(start_time)
        start_index = bisect.bisect_left(times, start_time) - 1
        bucketed_revenue[start_index] += revenue

    return bucketed_revenue

def get_bucketed_times(spaces, times):
    bucketed_times = [0] * (len(times) - 1)

    for id, start_time, end_time in spaces:
        if end_time is None:
            continue

        start_time = local_to_utc(start_time)
        end_time = local_to_utc(end_time)
        start_index = bisect.bisect_left(times, start_time) - 1
        end_index = bisect.bisect_right(times, end_time)

        if start_index < 0:
            start_index = 0
            start_time = times[start_index]
        if end_index == len(times):
            end_index = end_index - 1
            end_time = times[end_index]

        for i in range(start_index, end_index):
            if (start_index) == (end_index - 1):
                bucketed_times[i] += (end_time - start_time).total_seconds()
            elif i == start_index:
                bucketed_times[i] += (times[i+1] - start_time).total_seconds()
            elif i == (end_index - 1):
                bucketed_times[i] += (end_time - times[i]).total_seconds()
            else:
                bucketed_times[i] += (times[i+1] - times[i]).total_seconds()

    return bucketed_times

def local_to_utc(local_dt):
    return local_tz.localize(local_dt, is_dst=None).astimezone(pytz.utc)

def utc_to_local_stringify(utc_dt):
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')
