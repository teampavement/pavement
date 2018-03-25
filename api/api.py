#!/usr/bin/env python3
import bisect, operator, pytz
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
    params = request.get_json()
    if params is None:
        params = {}

    datetime_range = get_datetime_range(params)
    filters = [
        utc_to_local_stringify(datetime_range['start']) < ApTransactions.expiry_date,
        utc_to_local_stringify(datetime_range['end']) > ApTransactions.purchased_date
        ]

    if 'parking_spaces' in params:
        parking_spaces = params['parking_spaces']
        filters.append(ApTransactions.stall.in_(params['parking_spaces']))

    spaces = ApTransactions.query.with_entities(
        ApTransactions.stall, ApTransactions.purchased_date, ApTransactions.expiry_date
        ).filter(*filters).all()

    time_buckets = get_time_buckets(datetime_range)
    spaces = get_bucketed_spaces(spaces, time_buckets)

    return jsonify({
        'data': [{
            'timestamp': timestamp,
            'value': len(spaces[i]),
            'parking_spaces': [s for s in spaces[i]]
            } for i, timestamp in enumerate(time_buckets)]
    })

def get_bucketed_spaces(spaces, times):
    bucketed_spaces = [[] for _ in times]
    for id, start_time, end_time in spaces:
        if end_time is None:
            continue

        start_time = local_to_utc(start_time)
        end_time = local_to_utc(end_time)

        for i in range(bisect.bisect_left(times, start_time) - 1, bisect.bisect_right(times, end_time)):
            bucketed_spaces[i].append(id)

    return bucketed_spaces

@app.route('/parking-revenue', methods = ['POST'])
def get_parking_revenue():
    #SELECT purchased_date, expiry_date, revenue WHERE expiry_date > start_time AND purchased_date < end_time AND stall in (spaces_list)
    pass

@app.route('/parking-time', methods = ['POST'])
def get_parking_time():
    #SELECT purchased_date, expiry_date WHERE expiry_date > start_time AND purchased_date < end_time AND stall in (spaces_list)
    pass

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

def get_time_buckets(datetime_range):
    time_diff = (datetime_range['end'] - datetime_range['start']).total_seconds()
    if time_diff <= 259200: # 3 days
        return generate_times(datetime_range, timedelta(hours=1))
    elif time_diff > 259200 and time_diff <= 2678400: # 3-31 days
        return generate_times(datetime_range, timedelta(days=1))
    else:
        return generate_times(datetime_range, timedelta(weeks=1))

def generate_times(datetime_range, delta):
    times = []
    curr = datetime_range['start']
    while curr < datetime_range['end']:
        times.append(curr)
        curr += delta
    return times

def local_to_utc(local_dt):
    return local_tz.localize(local_dt, is_dst=None).astimezone(pytz.utc)

def utc_to_local_stringify(utc_dt):
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')
