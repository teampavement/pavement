#!/usr/bin/env python3
import config, csv, glob, re, psycopg2
from datetime import datetime
from dateutil import parser
from pytz import timezone

ticket = 0
pay_station = 1
stall = 2
license_plate = 3
purchased_date = 4
expiry_date = 5
payment_type = 6
transaction_type = 7
coupon_code = 8
excess_payment = 9
change_issued = 10
refund_ticket = 11
total_collections = 12
revenue = 13
rate_name = 14
hours_paid = 15
zone = 16
new_rate_weekday = 17
new_revenue_weekday = 18
new_rate_weekend = 19
new_revenue_weekend = 20
passport_tran = 21
merchant_tran = 22
parker_id = 23
conv_revenue = 24
validation_revenue = 25
transaction_fee = 26
card_type = 27
method = 28

def main():
    db = psycopg2.connect(
        host=config.Db.host,
        user=config.Db.user,
        password=config.Db.pw,
        dbname=config.Db.name)

    parse_2016(db)
    parse_2017(db)

def parse_2016(db):
    cursor = db.cursor()

    months = ['January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December']

    for month in months:
        print('Parsing ' + month + ' 2016...\n')

        with open ('AP Revenue - ' + month + ' 2016.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                del row[4:7] # delete day, date, time
                del row[16:19] # delete extra zone fields
                row = strip_na(row)
                row[purchased_date] = get_date_time(row[4])
                row[expiry_date] = get_date_time(row[5])
                row = strip_currencies(row, 9, 15)
                row.extend([None] * 7)
                row.append('its')

                cursor.execute("""INSERT INTO ap_transactions
                        (ticket, pay_station, stall, license_plate,
                        purchased_date, expiry_date, payment_type,
                        transaction_type, coupon_code, excess_payment,
                        change_issued, refund_ticket, total_collections,
                        revenue, rate_name, hours_paid, zone, new_rate_weekday,
                        new_revenue_weekday, new_rate_weekend,
                        new_revenue_weekend, passport_tran, merchant_tran,
                        parker_id, conv_revenue, validation_revenue,
                        transaction_fee, card_type, method)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s)""",
                        row)

    db.commit()
    cursor.close()

def parse_2017(db):
    parse_app(db)
    parse_ips(db)
    parse_its(db)

def parse_app(db):
    cursor = db.cursor()

    print('Parsing 2017 App data...\n')
    with open('2017_App_Transaction_Report.csv') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            row = strip_currencies(row, 10, 15)
            dbRow = [None] * 29
            dbRow[passport_tran] = row[1]
            dbRow[merchant_tran] = row[2]
            dbRow[parker_id] = row[3]
            dbRow[rate_name] = row[4]
            dbRow[zone] = row[6]
            dbRow[stall] = row[7]
            dbRow[purchased_date] = get_date_time(row[8])
            dbRow[expiry_date] = get_date_time(row[9])
            dbRow[conv_revenue] = row[11]
            dbRow[validation_revenue] = row[12]
            dbRow[transaction_fee] = row[13]
            dbRow[revenue] = row[14] # Net Revenue
            dbRow[payment_type] = row[15]
            dbRow[card_type] = row[16]
            dbRow[method] = 'app'

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate, purchased_date,
                    expiry_date, payment_type, transaction_type, coupon_code,
                    excess_payment, change_issued, refund_ticket,
                    total_collections, revenue, rate_name, hours_paid, zone,
                    new_rate_weekday, new_revenue_weekday, new_rate_weekend,
                    new_revenue_weekend, passport_tran, merchant_tran,
                    parker_id, conv_revenue, validation_revenue,
                    transaction_fee, card_type, method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def parse_ips(db):
    cursor = db.cursor()

    print('Parsing 2017 IPS data...\n')
    with open('2017_IPSGroup_Transaction_Report.csv') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            dbRow = [None] * 29
            dbRow[zone] = row[2]
            dbRow[pay_station] = row[5]
            dbRow[stall] = row[7]
            dbRow[license_plate] = row[8]
            dbRow[transaction_type] = row[9]
            dbRow[card_type] = row[12]
            dbRow[purchased_date] = get_date_time(row[0] + ' ' + row[1])
            dbRow[expiry_date] = get_date_time(row[13])
            dbRow[revenue] = row[22]
            dbRow[method] = 'ips'

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate, purchased_date,
                    expiry_date, payment_type, transaction_type, coupon_code,
                    excess_payment, change_issued, refund_ticket,
                    total_collections, revenue, rate_name, hours_paid, zone,
                    new_rate_weekday, new_revenue_weekday, new_rate_weekend,
                    new_revenue_weekend, passport_tran, merchant_tran,
                    parker_id, conv_revenue, validation_revenue,
                    transaction_fee, card_type, method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def parse_its(db):
    cursor = db.cursor()

    for itsFile in glob.glob('2017_ITS*.csv'):
        print('Parsing ' + itsFile + '...\n')
        with open (itsFile) as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                row = strip_currencies(strip_na(row), 9, 14)
                purchased = get_date_time(row[4])
                expired = get_date_time(row[5])

                dbRow = [row[0], row[1], row[2], row[3], purchased, expired,
                row[6], row[7], row[8], row[9], row[10], row[11], row[12],
                row[13], row[14]]
                dbRow.extend([None] * 13)
                dbRow.append('its')

                cursor.execute("""INSERT INTO ap_transactions
                        (ticket, pay_station, stall, license_plate,
                        purchased_date, expiry_date, payment_type,
                        transaction_type, coupon_code, excess_payment,
                        change_issued, refund_ticket, total_collections,
                        revenue, rate_name, hours_paid, zone, new_rate_weekday,
                        new_revenue_weekday, new_rate_weekend,
                        new_revenue_weekend, passport_tran, merchant_tran,
                        parker_id, conv_revenue, validation_revenue,
                        transaction_fee, card_type, method)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s)""",
                        dbRow)

    db.commit()
    cursor.close()

def get_date_time(val):
    if val:
        val = timezone('America/New_York').localize(parser.parse(val, ignoretz=True))

    return val

def strip_letters(val):
    if val:
        val = re.sub('[a-zA-Z]', '', val).rstrip()

    return val

def strip_na(row):
    row = [val.strip('N/A') for val in row]
    for i, val in enumerate(row):
        val = val.strip('N/A')
        if not val:
            val = None
        row[i] = val

    return row

def strip_currencies(row, start, end):
    for i in range(start, end):
        row[i] = row[i].strip('$')

    return row

if __name__ == '__main__':
    main()
