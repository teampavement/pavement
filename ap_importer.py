#!/usr/bin/env python3
import config, csv, glob, os, sys, re, MySQLdb
from datetime import datetime

ticket = 0
pay_station = 1
stall = 2
license_plate = 3
day = 4
date = 5
time = 6
purchased_date = 7
expiry_date = 8
payment_type = 9
transaction_type = 10
coupon_code = 11
excess_payment = 12
change_issued = 13
refund_ticket = 14
total_collections = 15
revenue = 16
rate_name = 17
hours_paid = 18
zone = 19
new_rate_weekday = 20
new_revenue_weekday = 21
new_rate_weekend = 22
new_revenue_weekend = 23
passport_tran = 24
merchant_tran = 25
parker_id = 26
conv_revenue = 27
validation_revenue = 28
transaction_fee = 29
card_type = 30
method = 31

def main():
    db = MySQLdb.connect(
        host=config.db['host'],
        user=config.db['user'],
        passwd=config.db['pass'],
        db=config.db['name'])

    #parse2016(db) needs to be updated if used again
    parse2017(db)

def parse2016(db):
    cursor = db.cursor()

    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    for month in months:
        with open ('AP Revenue - ' + month + ' 2016.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                row = stripNA(row)
                row = cleanDateTimes(row)
                row = stripCurrencies(row, 12, 18)
                del row[19:22]

                cursor.execute("""INSERT INTO ap_transactions
                        (ticket, pay_station, stall, license_plate, day, date, time, purchased_date, expiry_date, payment_type, transaction_type, coupon_code, excess_payment, change_issued, refund_ticket, total_collections, revenue, rate_name, hours_paid, zone, new_rate_weekday, new_revenue_weekday, new_rate_weekend, new_revenue_weekend, passport_tran, merchant_tran, parker_id, conv_revenue, validation_revenue, transaction_fee, card_type, method)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        dbRow)

    db.commit()
    cursor.close()

def parse2017(db):
    parseApp(db)
    parseIPS(db)
    parseITS(db)

def parseApp(db):
    cursor = db.cursor()

    with open('2017_App_Transaction_Report.csv') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            row = stripCurrencies(row, 10, 15)
            dbRow = [None] * 32
            dbRow[passport_tran] = row[1]
            dbRow[merchant_tran] = row[2]
            dbRow[parker_id] = row[3]
            dbRow[rate_name] = row[4]
            dbRow[zone] = row[6]
            dbRow[stall] = row[7]
            dbRow[purchased_date] = reorderDateTime(re.sub(' +', ' ' ,row[8]), False)
            dbRow[expiry_date] = reorderDateTime(re.sub(' +', ' ' ,row[9]), False)
            dbRow[conv_revenue] = row[11]
            dbRow[validation_revenue] = row[12]
            dbRow[transaction_fee] = row[13]
            dbRow[revenue] = row[14] # Net Revenue
            dbRow[payment_type] = row[15]
            dbRow[card_type] = row[16]
            dbRow[method] = 'app'

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate, day, date, time, purchased_date, expiry_date, payment_type, transaction_type, coupon_code, excess_payment, change_issued, refund_ticket, total_collections, revenue, rate_name, hours_paid, zone, new_rate_weekday, new_revenue_weekday, new_rate_weekend, new_revenue_weekend, passport_tran, merchant_tran, parker_id, conv_revenue, validation_revenue, transaction_fee, card_type, method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def parseIPS(db):
    cursor = db.cursor()

    with open('2017_IPSGroup_Transaction_Report.csv') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            purchased = datetime.strptime(row[0] + ' ' + row[1], '%m/%d/%y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') # handle AM/PM
            dbRow = [None] * 32
            dbRow[zone] = row[2]
            dbRow[pay_station] = row[5]
            dbRow[stall] = row[7]
            dbRow[license_plate] = row[8]
            dbRow[transaction_type] = row[9]
            dbRow[card_type] = row[12]
            dbRow[expiry_date] = reorderDateTime(row[13], False)
            dbRow[revenue] = row[22]
            dbRow[method] = 'ips'

            cursor.execute("""INSERT INTO ap_transactions
                    (ticket, pay_station, stall, license_plate, day, date, time, purchased_date, expiry_date, payment_type, transaction_type, coupon_code, excess_payment, change_issued, refund_ticket, total_collections, revenue, rate_name, hours_paid, zone, new_rate_weekday, new_revenue_weekday, new_rate_weekend, new_revenue_weekend, passport_tran, merchant_tran, parker_id, conv_revenue, validation_revenue, transaction_fee, card_type, method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    dbRow)

    db.commit()
    cursor.close()

def parseITS(db):
    cursor = db.cursor()

    for itsFile in glob.glob('2017_ITS*.csv'):
        with open (itsFile) as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                row = stripNA(row)
                row = stripCurrencies(row, 9, 14)
                purchased = reorderDateTime(row[4])
                expired = reorderDateTime(row[5])

                dbRow = [row[0], row[1], row[2], row[3], None, None, None, purchased, expired, row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14]]
                dbRow.extend([None] * 13)
                dbRow.append('its')

                cursor.execute("""INSERT INTO ap_transactions
                        (ticket, pay_station, stall, license_plate, day, date, time, purchased_date, expiry_date, payment_type, transaction_type, coupon_code, excess_payment, change_issued, refund_ticket, total_collections, revenue, rate_name, hours_paid, zone, new_rate_weekday, new_revenue_weekday, new_rate_weekend, new_revenue_weekend, passport_tran, merchant_tran, parker_id, conv_revenue, validation_revenue, transaction_fee, card_type, method)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        dbRow)

    db.commit()
    cursor.close()

def stripLetters(val):
    if val:
        val = re.sub('[a-zA-Z]', '', val).rstrip()
    return val

def reorderDate(val):
    if val:
        try:
            val = datetime.strptime(val, '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            val = datetime.strptime(val, '%m/%d/%y').strftime('%Y-%m-%d')
    return val

def reorderDateTime(val, seconds = True):
    if val:
        val = stripLetters(val)

        try:
            if seconds:
                val = datetime.strptime(val, '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            else:
                val = datetime.strptime(val, '%m/%d/%Y %H:%M').strftime('%Y-%m-%d %H:%M')
        except ValueError:
            if seconds:
                val = datetime.strptime(val, '%m/%d/%y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            else:
                val = datetime.strptime(val, '%m/%d/%y %H:%M').strftime('%Y-%m-%d %H:%M')

    return val

def stripNA(row):
    row = [val.strip('N/A') for val in row]
    for i, val in enumerate(row):
        val = val.strip('N/A')
        if not val:
            val = None
        row[i] = val

    return row

def cleanDateTimes(row):
    if row[5]:
        try:
            row[5] = datetime.strptime(row[5], '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            row[5] = datetime.strptime(row[5], '%m/%d/%y').strftime('%Y-%m-%d')
    if row[6]:
        row[6] = re.sub('[a-zA-Z]', '', row[6]).rstrip()
    if row[7]:
        purchased = re.sub('[a-zA-Z]', '', row[7]).rstrip()
        row[7] = datetime.strptime(purchased, '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    if row[8]:
        expired = re.sub('[a-zA-Z]', '', row[8]).rstrip()
        row[8] = datetime.strptime(expired, '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

    return row

def stripCurrencies(row, start, end):
    for i in range(start, end):
        row[i] = row[i].strip('$')

    return row

if __name__ == '__main__':
    main()
