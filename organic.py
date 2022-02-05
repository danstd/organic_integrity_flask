from flask import Flask, redirect, render_template, request, url_for
import requests
import json
from api_key_get import key_get
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
import io
import base64

# Set plot parameters
plt.rcParams.update({"font.size": 22})

app = Flask(__name__)
app.debug = True

SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
    username="flaskuser",
    password="dersAGef3rover",

    hostname="127.0.0.1",
    databasename="organic_integrity",
)
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config["SQLALCHEMY_POOL_RECYCLE"] = 299
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
#app.secret_key = ""
#login_manager = LoginManager()
#login_manager.init_app(app)
#migrate = Migrate(app, db)

#@app.route("/", methods=["GET", "POST"])
@app.route("/", methods=["GET"])
def index():
    if request.method != "GET":
        return render_template("main_page.html")
    else:
        # Query to get number of operations by status and by country
        op_return = db.session.query(
            OrganicOperation.opPA_country.label("Country"),
            OrganicOperation.op_status,
            db.func.count(OrganicOperation.op_nopOpID).label("op_count")
        ).select_from(OrganicOperation).order_by(OrganicOperation.opPA_country).group_by(
            OrganicOperation.opPA_country,
            OrganicOperation.op_status)

        df_pivot = pd.DataFrame(op_return).pivot_table(index="Country", 
                                          columns="op_status", 
                                          values="op_count",
                                          aggfunc="sum", fill_value=0,
                                         margins=True)
        
        df_pivot.reset_index(inplace=True)

        df_cols = df_pivot.columns.tolist()

        # Create certification change plot.
        certification_date = db.session.query(
            OrganicOperation.op_statusEffectiveDate,
            OrganicOperation.op_status,
            db.func.count(OrganicOperation.op_nopOpID).label("op_count")
            ).select_from(OrganicOperation).group_by(
            OrganicOperation.op_statusEffectiveDate,
            OrganicOperation.op_status)
        
        cert_date_df = pd.DataFrame(certification_date)

        cert_date_df["year"] = pd.DatetimeIndex(cert_date_df["op_statusEffectiveDate"]).year

        cert_date_df = cert_date_df.rename(columns={"op_status": "Certification Status"})
        # Further aggregate by month
        cert_date_df["op_statusEffectiveDate"] = cert_date_df["op_statusEffectiveDate"].apply(lambda x: x.replace(day=1))

        cert_date_df = cert_date_df.loc[cert_date_df["year"] > datetime.datetime.now().year-10].groupby(["Certification Status","op_statusEffectiveDate"], as_index=False)["op_count"].sum()

        sns.set_style("darkgrid")
        sns.set_palette("Set3")
        sns.relplot(data=cert_date_df, x="op_statusEffectiveDate", y="op_count", kind="line", hue="Certification Status",height=10, aspect=1.5, linewidth=3)

        plt.title("Certification Changes Over Time")
        plt.xlabel("Date")
        plt.ylabel("Monthly Change")

        #plt.show()
        # From https://stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
        status_date_url = "static\\images\\certification_date.png"
        plt.savefig(status_date_url, bbox_inches="tight", pad_inches=0.3)

        return render_template(
            "main_page.html",
            organic_display=df_pivot.values.tolist(),
            organic_cols=df_cols,
            status_date_url=status_date_url
        )

# Models
class OrganicOperation(db.Model):
    __tablename__ = "organic_operation"
    op_certifierName = db.Column(db.String(256))
    op_nopOpID = db.Column(db.String(256), primary_key=True)
    op_name = db.Column(db.String(256))
    op_otherNames = db.Column(db.String(256))
    op_clientID = db.Column(db.String(256))
    op_contFirstName = db.Column(db.String(256))
    op_contLastName = db.Column(db.String(256))
    op_status = db.Column(db.String(256))
    op_statusEffectiveDate = db.Column(db.DateTime)
    op_nopAnniversaryDate = db.Column(db.DateTime)
    op_lastUpdatedDate = db.Column(db.DateTime)
    opSC_CR = db.Column(db.String(256))
    opSC_CR_ED = db.Column(db.DateTime)
    opSC_LS = db.Column(db.String(256))
    opSC_LS_ED = db.Column(db.DateTime)
    opSC_WC = db.Column(db.String(256))
    opSC_WC_ED = db.Column(db.DateTime)
    opSC_HANDLING = db.Column(db.String(256))
    opSC_HANDLING_ED = db.Column(db.DateTime)
    opPA_line1 = db.Column(db.String(256))
    opPA_line2 = db.Column(db.String(256))
    opPA_city = db.Column(db.String(256))
    opPA_state = db.Column(db.String(256))
    opPA_country = db.Column(db.String(256))
    opPA_zip = db.Column(db.String(256))
    opPA_countyCode = db.Column(db.String(256))
    opPA_county = db.Column(db.String(256))
    opMA_line1 = db.Column(db.String(256))
    opMA_line2 = db.Column(db.String(256))
    opMA_city = db.Column(db.String(256))
    opMA_state = db.Column(db.String(256))
    opMA_country = db.Column(db.String(256))
    opMA_zip = db.Column(db.String(256))
    opMA_countyCode = db.Column(db.String(256))
    opMA_county = db.Column(db.String(256))
    op_phone = db.Column(db.String(256))
    op_email = db.Column(db.String(256))
    op_url = db.Column(db.String(256))
    op_opExtraInfo = db.Column(db.String(256))
    opEx_broker = db.Column(db.String(256))
    opEx_csa = db.Column(db.String(256))
    opEx_copacker = db.Column(db.String(256))
    opEx_dairy = db.Column(db.String(256))
    opEx_distributor = db.Column(db.String(256))
    opEx_marketerTrader = db.Column(db.String(256))
    opEx_restaurant = db.Column(db.String(256))
    opEx_retailer = db.Column(db.String(256))
    opEx_poultry = db.Column(db.String(256))
    opEx_privateLabeler = db.Column(db.String(256))
    opEx_slaughterHouse = db.Column(db.String(256))
    opEx_storage = db.Column(db.String(256))
    opEx_growerGroup = db.Column(db.String(256))
    opCert_url = db.Column(db.String(256))


class OrganicItem(db.Model):
    __tablename__ = "organic_item"
    ci_artID = db.Column(db.Integer, primary_key=True) # Artificial primary key added in.
    ci_nopOpID = db.Column(db.String(256), db.ForeignKey(OrganicOperation.op_nopOpID))
    ci_certNumber = db.Column(db.String(256))
    ci_nopScope = db.Column(db.String(256))
    ci_nopCategory = db.Column(db.String(256))
    ci_nopCatID = db.Column(db.String(256))
    ci_nopCatName = db.Column(db.String(256))
    ci_nopItemID = db.Column(db.String(256))
    ci_itemList = db.Column(db.String(256))
    ci_varieties = db.Column(db.String(256))
    ci_status = db.Column(db.String(256))
    ci_statusEffectiveDate = db.Column(db.String(256))
    ci_organic100 = db.Column(db.String(256))
    ci_organic = db.Column(db.String(256))
    ci_madeWithOrganic = db.Column(db.String(256))
    fk_operation_ID = db.relationship("OrganicOperation", foreign_keys=ci_nopOpID)




