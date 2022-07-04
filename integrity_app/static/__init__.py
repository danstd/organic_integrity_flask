from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import sep

SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
    username="flaskuser",
    password="dersAGef3rover",

    hostname="127.0.0.1",
    databasename="organic_integrity",
)

app = Flask("integrity_app")

app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config["SQLALCHEMY_POOL_RECYCLE"] = 299
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

import integrity_app.integrity_model
import integrity_app.integrity_routes
import integrity_app.organic_processing
