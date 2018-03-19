#!/usr/bin/env python3
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import DOUBLE
from flask import jsonify
import enum
db = SQLAlchemy()

class DayEnum(enum.Enum):
    Sun = 0
    Mon = 1
    Tue = 2
    Wed = 3
    Thu = 4
    Fri = 5
    Sat = 6

class Table:
    def to_json(self):
        """Convert query result to JSON"""
        return jsonify({c.name: getattr(self, c.name) for c in self.__table__.columns})

    @staticmethod
    def list_to_json(results):
        """Convert list of query results to JSON"""
        return jsonify([{c.name: getattr(res, c.name) for c in res.__table__.columns} for res in results])

    def get_column(self, column):
        return jsonify([col[0] for col in self.query.with_entities(column).distinct(column)])

class ApTransactions(Table, db.Model):
    __tablename__ = 'ap_transactions'
    id = db.Column(db.BIGINT, primary_key=True)
    ticket = db.Column(db.Integer)
    pay_station = db.Column(db.VARCHAR(length=50))
    stall = db.Column(db.VARCHAR(length=50))
    license_plate = db.Column(db.VARCHAR(length=8))
    day = db.Column(db.Enum(DayEnum))
    date = db.Column(db.DATE)
    time = db.Column(db.TIME)
    purchased_date = db.Column(db.DATETIME, nullable=False)
    expiry_date = db.Column(db.DATETIME)
    payment_type = db.Column(db.VARCHAR(length=50))
    transaction_type = db.Column(db.VARCHAR(length=50))
    coupon_code = db.Column(db.VARCHAR(length=50))
    excess_payment = db.Column(db.DECIMAL(precision=10, scale=2))
    change_issued = db.Column(db.DECIMAL(precision=10, scale=2))
    refund_ticket = db.Column(db.DECIMAL(precision=10, scale=2))
    total_collections = db.Column(db.DECIMAL(precision=10, scale=2))
    revenue = db.Column(db.DECIMAL(precision=10, scale=2))
    rate_name = db.Column(db.VARCHAR(length=50))
    hours_paid = db.Column(db.DECIMAL(precision=4, scale=2))
    zone = db.Column(db.VARCHAR(length=50))
    new_rate_weekday = db.Column(DOUBLE)
    new_revenue_weekday = db.Column(DOUBLE)
    new_rate_weekend = db.Column(DOUBLE)
    new_revenue_weekend = db.Column(DOUBLE)
    passport_tran = db.Column(db.Integer)
    merchant_tran = db.Column(db.BIGINT)
    parker_id = db.Column(db.Integer)
    conv_revenue = db.Column(db.DECIMAL(precision=10, scale=2))
    validation_revenue = db.Column(db.DECIMAL(precision=10, scale=2))
    transaction_fee = db.Column(db.DECIMAL(precision=10, scale=2))
    card_type = db.Column(db.VARCHAR(length=50))
    method = db.Column(db.VARCHAR(length=50))
