#!/usr/bin/env python3
import config, csv, glob, re, psycopg2, pytz
from datetime import datetime, timezone, time
from dateutil import parser

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
parking_day = 29

monday = 0
tuesday = 1
wednesday = 2
thursday = 3
friday = 4
saturday = 5
sunday = 6

after_hours_start = time(4, 0, tzinfo=timezone.utc) # 12am
after_hours_end = time(13, 0, tzinfo=timezone.utc) # 9am

def main():
    db = psycopg2.connect(
        host=config.Db.host,
        user=config.Db.user,
        password=config.Db.pw,
        dbname=config.Db.name)

    parse_2016(db)
    parse_2017(db)
    parse_2018(db)

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
                row.append(get_parking_day(row[purchased_date]))

                cursor.execute("""INSERT INTO ap_transactions
                        (ticket, pay_station, stall, license_plate,
                        purchased_date, expiry_date, payment_type,
                        transaction_type, coupon_code, excess_payment,
                        change_issued, refund_ticket, total_collections,
                        revenue, rate_name, hours_paid, zone, new_rate_weekday,
                        new_revenue_weekday, new_rate_weekend,
                        new_revenue_weekend, passport_tran, merchant_tran,
                        parker_id, conv_revenue, validation_revenue,
                        transaction_fee, card_type, method, parking_day)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s)""",
                        row)

    db.commit()
    cursor.close()

def parse_2017(db):
    parse_app(db, '2017_App_Transaction_Report.csv')
    parse_ips(db, '2017_IPSGroup_Transaction_Report.csv')
    parse_its_2017(db, '2017_ITS*.csv')

def parse_2018(db):
    parse_app(db, '2018_App_Transaction_Report_Jan-April.csv')
    parse_ips(db, 'IPS_Transactions_Jan-Apr2018.csv')
    parse_its_2018(db, '2017_ITS*.csv')

def parse_app(db, file):
    cursor = db.cursor()

    print('Parsing App data for ' + file + '...\n')
    with open(file) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            row = strip_currencies(row, 10, 15)
            dbRow = [None] * 30
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
            dbRow[parking_day] = get_parking_day(dbRow[purchased_date])

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate, purchased_date,
                    expiry_date, payment_type, transaction_type, coupon_code,
                    excess_payment, change_issued, refund_ticket,
                    total_collections, revenue, rate_name, hours_paid, zone,
                    new_rate_weekday, new_revenue_weekday, new_rate_weekend,
                    new_revenue_weekend, passport_tran, merchant_tran,
                    parker_id, conv_revenue, validation_revenue,
                    transaction_fee, card_type, method, parking_day)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def parse_ips(db, file):
    cursor = db.cursor()

    print('Parsing IPS data for ' + file + '...\n')
    with open(file) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            dbRow = [None] * 30
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
            dbRow[parking_day] = get_parking_day(dbRow[purchased_date])

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate, purchased_date,
                    expiry_date, payment_type, transaction_type, coupon_code,
                    excess_payment, change_issued, refund_ticket,
                    total_collections, revenue, rate_name, hours_paid, zone,
                    new_rate_weekday, new_revenue_weekday, new_rate_weekend,
                    new_revenue_weekend, passport_tran, merchant_tran,
                    parker_id, conv_revenue, validation_revenue,
                    transaction_fee, card_type, method, parking_day)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def parse_its_2017(db, files):
    for itsFile in glob.glob(files):
        parse_its(db, itsFile)

def parse_its_2018(db, file):
    parse_its(db, 'ITS Transactions 2018.csv')

def parse_its(db, file):
    cursor = db.cursor()
    print('Parsing ITS data for ' + file + '...\n')
    with open (file) as f:
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
            dbRow.append(get_parking_day(purchased))

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate,
                    purchased_date, expiry_date, payment_type,
                    transaction_type, coupon_code, excess_payment,
                    change_issued, refund_ticket, total_collections,
                    revenue, rate_name, hours_paid, zone, new_rate_weekday,
                    new_revenue_weekday, new_rate_weekend,
                    new_revenue_weekend, passport_tran, merchant_tran,
                    parker_id, conv_revenue, validation_revenue,
                    transaction_fee, card_type, method, parking_day)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def get_date_time(val):
    if val:
        val = pytz.timezone('America/New_York').localize(parser.parse(val, ignoretz=True))

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

def get_parking_day(purchased_date):
    day = purchased_date.weekday()

    if is_after_hours(purchased_date):
        return shift_day_back(day)
    else:
        return day

def is_after_hours(purchased_date):
    return purchased_date.astimezone(pytz.utc).timetz() >= after_hours_start \
        and purchased_date.astimezone(pytz.utc).timetz() < after_hours_start

def shift_day_back(day):
    if day == monday:
        return sunday
    else:
        return day - 1

if __name__ == '__main__':
    main()
