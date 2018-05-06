#!/usr/bin/env python3
import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URI')

class LocalConfig(Config):
    DEBUG = True
    DEVELOPMENT = True

class ProductionConfig(Config):
    DEVELOPMENT = False
    DEBUG = False

class Db:
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    pw = os.getenv('DB_PASS')
    name = os.getenv('DB_NAME')

class DbRead:
    host = ''
    user = ''
    pw = ''
    name = 'pavement'

class DbWrite:
    host = ''
    user = ''
    pw = ''
    name = 'pavement'
