from integrity_app import app
from flask import Flask, redirect, render_template, request, url_for
import pandas as pd
import datetime
from os import sep

# Set file path variables
static = "integrity_app" + sep + "static" + sep
static_img = "integrity_app" + sep + "static" + sep + "images" + sep

#app = Flask(__name__)
#app.debug = True

@app.route("/", methods=["GET"])
def index():
    if request.method != "GET":
        return render_template("organic_main_page.html")
    else:
        
        return render_template(
            "organic_main_page.html")
        
#----------------------world route---------------------
@app.route("/world", methods=["GET"])
def world():
    if request.method != "GET":
        return render_template("main_page.html")
    else:
        # Get saved images and csv files.
        country_table = pd.read_csv(static + "op_status_country.csv")

        scopes_count = pd.read_csv(static + "scopes_count.csv")
        scope_card_keys = scopes_count.columns.to_list()
        scope_card_vals = scopes_count.values.tolist()
        
        # Convert to dictionary so the result can easily be put into cards.
        scope_cards = dict()
        for i in range(0,len(scope_card_keys)):
            scope_cards[scope_card_keys[i]] = scope_card_vals[0][i]

        scope_set = pd.read_csv(static + "scopes_combo.csv")
        scope_set = scope_set.fillna("")

        return render_template(
            "world.html",
            country_table=country_table.values.tolist(),
            country_table_cols=country_table.columns.to_list(),
            scope_cards=scope_cards,
            scopes_combo=scope_set.values.tolist(),
            scopes_combo_cols=scope_set.columns.to_list()
            )

#----------------------united_states route---------------------
@app.route("/united_states", methods=["GET"])
def united_states():
    if request.method != "GET":
        return render_template("main_page.html")
    else:
        # Get saved images and csv files.

        us_pivot = pd.read_csv(static + "us_table.csv")
        us_pivot = us_pivot.fillna("")

        us_scopes_return = pd.read_csv(static + "us_scopes_return.csv", dtype=str)
        # Must fix formatting
        for i in us_scopes_return.columns.to_list():
            us_scopes_return[i] = us_scopes_return[i].str.replace(".0","",regex=False)
            us_scopes_return[i] = us_scopes_return[i].str.replace("^0$","None",regex=True)

        us_scopes_return = us_scopes_return.fillna("")
        
        return render_template(
            "united_states.html",
            us_table=us_pivot.values.tolist(),
            us_table_cols=us_pivot.columns.to_list(),
            us_scopes_display=us_scopes_return.values.tolist())


#----------------------products route---------------------
@app.route("/products", methods=["GET"])
def products():
    if request.method != "GET":
        return render_template("main_page.html")
    else:
        top_items_crops=pd.read_csv(static + "top_items_crops.csv").values.tolist()
        top_items_livestock=pd.read_csv(static + "top_items_livestock.csv").values.tolist()
        top_items_handling=pd.read_csv(static + "top_items_handling.csv").values.tolist()
        top_items_wild=pd.read_csv(static + "top_items_wild.csv").values.tolist()

        return render_template(
            "products.html",
            top_items_crops=top_items_crops,
            top_items_livestock=top_items_livestock,
            top_items_handling=top_items_handling,
            top_items_wild=top_items_wild)
            
