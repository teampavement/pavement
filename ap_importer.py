#!/usr/bin/env python3
import csv, os, sys, re, MySQLdb
from datetime import datetime

def main():
    db = MySQLdb.connect(host='',
        user='',
        passwd='',
        db='pavement')
    cursor = db.cursor()

    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

    for month in months:
        with open ('AP Revenue - ' + month + ' 2016.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                row = stripNA(row)
                row = cleanDateTimes(row)
                row = stripCurrencies(row)
                del row[19:22]

                cursor.execute("""INSERT INTO ap_revenue
                        (ticket, pay_station, stall, license_plate, day, date, time, purchased_date, expiry_date, payment_type, transaction_type, coupon_code, excess_payment, change_issued, refund_ticket, total_collections, revenue, rate_name, hours_paid, zone, new_rate_weekday, new_revenue_weekday, new_rate_weekend, new_revenue_weekend)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        row)

    db.commit()
    cursor.close()

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
            row[5] = datetime.strptime(row[5], "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            row[5] = datetime.strptime(row[5], "%m/%d/%y").strftime("%Y-%m-%d")
    if row[6]:
        row[6] = re.sub('[a-zA-Z]', '', row[6]).rstrip()
    if row[7]:
        purchased = re.sub('[a-zA-Z]', '', row[7]).rstrip()
        row[7] = datetime.strptime(purchased, "%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    if row[8]:
        expired = re.sub('[a-zA-Z]', '', row[8]).rstrip()
        row[8] = datetime.strptime(expired, "%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

    return row

def stripCurrencies(row):
    for i in range(12, 18):
        row[i] = row[i].strip('$')

    return row

if __name__ == '__main__':
    main()
