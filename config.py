#!/usr/bin/env python3
import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    DEBUG = False
    TESTING = False

class LocalConfig(object):
    DEBUG = True
    DEVELOPMENT = True
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URI')

class ProductionConfig(Config):
    DEVELOPMENT = False
    DEBUG = False

class Db:
    host = 'localhost'
    user = 'pavement'
    pw = ''
    name = 'pavement'

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
