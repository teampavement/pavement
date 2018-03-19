#!/usr/bin/env python3
import operator
from datetime import datetime
from pathlib import Path
from flask import Flask, json, jsonify, abort, request
from tables import db, ApTransactions

app = Flask(__name__)
app.config.from_pyfile('../flask_config.py') # TODO: load this from main config.py object instead
db.init_app(app)

@app.route('/parking-spots', methods = ['GET'])
def get_parking_spots():
    pass

@app.route('/parking-occupancy', methods = ['POST'])
def get_parking_occupancy():
    params = request.get_json()
    if params is None:
        params = {}

    datetime_range = get_datetime_range(params)
    filters = [ApTransactions.purchased_date >= datetime_range['start'], ApTransactions.expiry_date <= datetime_range['end']]
    if 'parking_spots' in params:
        parking_spots = params['parking_spots']
        filters.append(ApTransactions.stall.in_(params['parking_spots']))
    else:
        parking_spots = None

    spots = ApTransactions.query.with_entities(ApTransactions.stall).distinct(ApTransactions.stall).all()

    return jsonify({
        'datetime_range': {
            'start': datetime_range['start'],
            'end': datetime_range['end']
        },
        'parking_spots': parking_spots,
        'chart_type': 'test',
        'data': {
            'timestamp': datetime.now(),
            'value': len(spots),
            'spot_ids': [s[0] for s in spots]
        }
    })

@app.route('/parking-revenue', methods = ['POST'])
def get_parking_revenue():
    #SELECT purchased_date, expiry_date, revenue WHERE expiry_date > start_time AND purchased_date < end_time AND stall in (spots_list)
    pass

@app.route('/parking-time', methods = ['POST'])
def get_parking_time():
    #SELECT purchased_date, expiry_date WHERE expiry_date > start_time AND purchased_date < end_time AND stall in (spots_list)
    pass

def get_datetime_range(params):
    if 'datetime_range' not in params:
        start_date = datetime.min
        end_date = datetime.max
    else:
        if 'start' not in params['datetime_range']:
            start_date = datetime.min
        else:
            # convert
            pass
        if 'end' not in params['datetime_range']:
            end_date = datetime.max
        else:
            #convert
            pass

    return  {'start' : start_date, 'end': end_date}
