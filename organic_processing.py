from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import seaborn as sns
import datetime
import matplotlib.pyplot as plt

# Set plot parameters
plt.rcParams.update({"font.size": 22})

SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
    username="flaskuser",
    password="dersAGef3rover",

    hostname="127.0.0.1",
    databasename="organic_integrity",
)

app = Flask("organic")

app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config["SQLALCHEMY_POOL_RECYCLE"] = 299
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)


# -----------------Models--------------------------------
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
    # op_opExtraInfo = db.Column(db.String(256))
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


#----------------World page views-------------------------

# op_status_country.csv------------------------
# Query to get number of operations by status and by country
op_return = db.session.query(
    OrganicOperation.opPA_country.label("Country"),
    OrganicOperation.op_status,
    db.func.count(OrganicOperation.op_nopOpID).label("op_count")
    ).select_from(OrganicOperation).order_by(OrganicOperation.opPA_country).group_by(
    OrganicOperation.opPA_country,
    OrganicOperation.op_status)

# Pivot the result by certification status
country_table = pd.DataFrame(op_return).pivot_table(
    index="Country", 
    columns="op_status", 
    values="op_count",
    aggfunc="sum", fill_value=0,
    margins=True)
    
country_table.reset_index(inplace=True)

# Change names for formatting - put entries without a country listed last, and the summary row first.
country_table.loc[country_table["Country"]=="", "Country"] = "__No Country Name"
country_table.loc[country_table["Country"]=="All", "Country"] = "AAALL"

country_table["Country"] = country_table["Country"].str.replace("the", "The", regex=False)

country_table.sort_values("Country",inplace=True)
country_table.loc[country_table["Country"]=="AAALL", "Country"] = "All Countries"
country_table.loc[country_table["Country"]=="__No Country Name", "Country"] = "No Country Name"

country_table.to_csv("static\\op_status_country.csv", index=False)

# certification_date.png---------------------------
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

sns.set_style("whitegrid")
sns.set_palette("Set1")
sns.set_context("poster")
sns.relplot(data=cert_date_df, x="op_statusEffectiveDate",
    y="op_count",
    kind="line",
    hue="Certification Status",
    height=12,
    aspect=1.5,
    linewidth=3)

#plt.title("Certification Changes Over Time")
plt.xlabel("Date")
plt.ylabel("Monthly Change")

# From https://stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
status_date_url = "static\\images\\certification_date.png"
plt.savefig(status_date_url, bbox_inches="tight", pad_inches=0.3)


# certification_date_basic.png---------------------
# Create basic certification change plot - certified minus surrendered, suspended, etc..
trend_df = pd.DataFrame(certification_date)
trend_df['year'] = pd.DatetimeIndex(trend_df['op_statusEffectiveDate']).year
trend_df['op_statusEffectiveDate'] = trend_df['op_statusEffectiveDate'].apply(lambda x: x.replace(day=1))
trend_df = trend_df.loc[trend_df['year'] > 2018]
trend_df = trend_df.loc[trend_df["op_status"] != "Applied; APEDA Certified"]

# Group by operation count.
trend_df = trend_df.groupby(['op_status','op_statusEffectiveDate'], as_index=False)['op_count'].sum()

# Pivot result on operation status
trend_df = trend_df.pivot_table(
    index="op_statusEffectiveDate", 
    columns="op_status", 
    values="op_count",
    aggfunc="sum", fill_value=0,
    margins=False)

# Get the trend - count of certified minus surrendered, suspended, and revoked.
trend_df["Trend"] = trend_df["Certified"] - (trend_df["Revoked"] + trend_df["Surrendered"] + trend_df["Suspended"])

sns.set_style('whitegrid')
sns.set_palette('dark')
sns.set_context("poster")
sns.relplot(data=trend_df, x='op_statusEffectiveDate', y='Trend', kind='line',height=10, aspect=1.5)

#plt.title('Certification Changes Over Time')
plt.xlabel('Date')
plt.ylabel('Monthly Change')
plt.xticks(rotation=30)

# From https://stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
simple_status_date_url = "static\\images\\certification_date_basic.png"
plt.savefig(simple_status_date_url, bbox_inches="tight", pad_inches=0.3)

# scopes_count.csv------------------------
# Global certification scope get
# Subqueries
handling_count = db.session.query(
    db.func.count(OrganicOperation.opSC_HANDLING
    ).label("Handling")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opSC_HANDLING == "Certified")

crops_count = db.session.query(
    db.func.count(OrganicOperation.opSC_CR
    ).label("Crops")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opSC_CR == "Certified")

livestock_count = db.session.query(
    db.func.count(OrganicOperation.opSC_LS
    ).label("Livestock")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opSC_LS == "Certified")

wild_count = db.session.query(
    db.func.count(OrganicOperation.opSC_WC
    ).label("Wild_Crops")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opSC_WC == "Certified")

# Union for single call to db.
scopes_return = (handling_count
    ).union(crops_count
    ).union(livestock_count
     ).union(wild_count
    ).all()

# Extract counts by scope.
scopes = ["Handling", "Crops", "Livestock", "Wild Crops"]
scopes_count = dict()
for i in range(0,len(scopes)):
    scopes_count[scopes[i]]= scopes_return[i][0]

pd.DataFrame(scopes_count, index=[0]).to_csv("static\\scopes_count.csv", index=False)

# scopes_combo.csv---------------------------------
# Get certified count combinations
# Get all scope certification columns
scope_set = db.session.query(
    OrganicOperation.opSC_HANDLING,
    OrganicOperation.opSC_CR,
    OrganicOperation.opSC_LS,
    OrganicOperation.opSC_WC,
    ).select_from(OrganicOperation
    ).all()

# Make a dataframe and drop any certifications that are not certified at all.
scope_set = pd.DataFrame(scope_set, columns=["H", "C", "L", "W"])

scope_set = scope_set.loc[
    (scope_set["H"] == "Certified") |
    (scope_set["C"] == "Certified") |
    (scope_set["L"] == "Certified") |
    (scope_set["W"] == "Certified")]

# Make a pseudo sparse matrix
scope_cols = scope_set.columns.tolist()
for i in scope_cols:
    scope_set[i] = scope_set[i].str.replace("Surrendered","")
    scope_set[i] = scope_set[i].str.replace("Suspended","")
    scope_set[i] = scope_set[i].str.replace("Certified",i)

# Group the combinations.
# Pandas on pyhtonanywhere is ignoring the as_index argument.
# Added reset_index to fix. As a result 'size' column is named '0'
scope_set = scope_set.groupby(["H", "C", "L", "W"], as_index=False).size().reset_index()
# Make a name column with meaning for the combinations.
#scope_set["Name"] = scope_set["H"] +  ", " + scope_set["C"] + ", " + scope_set["L"] + ", " + scope_set["W"]
#scope_set["Name"] = scope_set["Name"].str.replace(r'^(, )+|(, )+$', "", regex=True)
#scope_set["Name"] = scope_set["Name"].str.replace(r'(, )+', ", ", regex=True)

# Fix size column name
if "0" in scope_set.columns.to_list():
    scope_set = scope_set.rename(columns={"0":"size"})
elif 0 in scope_set.columns.to_list():
    scope_set = scope_set.rename(columns={0:"size"})
    
# Fix size column name
if "0" in scope_set.columns.to_list():
    scope_set = scope_set.rename(columns={"0":"size"})
elif 0 in scope_set.columns.to_list():
    scope_set = scope_set.rename(columns={0:"size"})
    
# Get the percentage of each combination.
total = sum(scope_set["size"])

scope_set["size"] = round(((scope_set["size"] / total)* 100),3)

scope_set.sort_values("size", ascending=False, inplace=True)

scope_set = scope_set[["size", "H", "C", "L", "W"]]

scope_set.rename(
    columns={"size":"Percentage",
            "H":"Handling (H)",
             "C":"Crops (C)",
             "L":"Livestock (L)",
             "W":"Wild Crops (W)"}
    ,inplace=True)
#scopes_combo_cols=["Handling (H)", "Crops (C)", "Livestock (L)", "Wild Crops (W)", "Percentage"]

scope_set = scope_set.fillna("")    
#scope_set.to_csv("static\\scopes_combo.csv", index=False, header=scopes_combo_cols)
scope_set.to_csv("static\\scopes_combo.csv", index=False)


#------------------------------united_states page view-------------------------------

# us_table.csv--------------------------
# Query to get number of operations by status and by state for US (and outlying islands)
us_return = db.session.query(
    OrganicOperation.opPA_state.label("State"),
    OrganicOperation.op_status,
    db.func.count(OrganicOperation.op_nopOpID).label("op_count")
    ).select_from(OrganicOperation).order_by(OrganicOperation.opPA_state).group_by(
    OrganicOperation.opPA_state,
    OrganicOperation.op_status).filter(OrganicOperation.opPA_country.like("%United States%")).all()

us_pivot = pd.DataFrame(us_return).pivot_table(index="State", 
    columns="op_status", 
    values="op_count",
    aggfunc="sum", fill_value=0,
    margins=True)
    
us_pivot.reset_index(inplace=True)

us_pivot.to_csv("static\\us_table.csv", index=False)

# us_scopes_return.csv-------------------------
# Query to get number of certified operations by scope for the US.
# Unsure how to optimize using sqlalchemy
# States subquery b/c lack of outer join
states_sub = db.session.query(
    db.func.distinct(OrganicOperation.opPA_state
    ).label("State")).select_from(OrganicOperation
    ).filter(OrganicOperation.opPA_country.like("%United States%")).subquery()

# Handling subquery
handling_sub = db.session.query(
    OrganicOperation.opPA_state,
    db.func.count(OrganicOperation.opSC_HANDLING).label("Handling")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opPA_country.like("%United States%")
    ).filter(OrganicOperation.opSC_HANDLING == "Certified"
    ).group_by(OrganicOperation.opPA_state
    ).subquery()

# Crops subquery
crops_sub = db.session.query(
    OrganicOperation.opPA_state,
    db.func.count(OrganicOperation.opSC_CR).label("Crops")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opPA_country.like("%United States%")
    ).filter(OrganicOperation.opSC_CR == "Certified"
    ).group_by(OrganicOperation.opPA_state
    ).subquery()

# Livestock subquery
livestock_sub = db.session.query(
    OrganicOperation.opPA_state,
    db.func.count(OrganicOperation.opSC_LS).label("Livestock")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opPA_country.like("%United States%")
    ).filter(OrganicOperation.opSC_LS == "Certified"
    ).group_by(OrganicOperation.opPA_state
    ).subquery()

# Wild crops subquery
wild_sub = db.session.query(
    OrganicOperation.opPA_state,
    db.func.count(OrganicOperation.opSC_WC).label("Wild_Crops")
    ).select_from(OrganicOperation
    ).filter(OrganicOperation.opPA_country.like("%United States%")
    ).filter(OrganicOperation.opSC_WC == "Certified"
    ).group_by(OrganicOperation.opPA_state
    ).subquery()

# Outer query
us_scopes_return = db.session.query(
    states_sub.c.State,
    handling_sub.c.Handling,
    crops_sub.c.Crops,
    livestock_sub.c.Livestock,
    wild_sub.c.Wild_Crops
    ).select_from(states_sub
    ).join(handling_sub, states_sub.c.State == handling_sub.c.opPA_state, isouter=True
    ).join(crops_sub, states_sub.c.State == crops_sub.c.opPA_state, isouter=True
    ).join(livestock_sub, states_sub.c.State == livestock_sub.c.opPA_state, isouter=True
    ).join(wild_sub, states_sub.c.State == wild_sub.c.opPA_state, isouter=True
    ).group_by(states_sub.c.State
    ).all()

us_scopes_return = pd.DataFrame(us_scopes_return).fillna("0")

us_scopes_return.to_csv("static\\us_scopes_return.csv", index=False)

# us_certification_date.png------------------------------
# Create US certification change plot.
us_date = db.session.query(
    OrganicOperation.op_statusEffectiveDate,
    OrganicOperation.op_status,
    db.func.count(OrganicOperation.op_nopOpID).label("op_count")
    ).select_from(OrganicOperation).group_by(
    OrganicOperation.op_statusEffectiveDate,
    OrganicOperation.op_status
    ).filter(OrganicOperation.opPA_country.like("%United States%")).all()
    
us_date_df = pd.DataFrame(us_date)

us_date_df["year"] = pd.DatetimeIndex(us_date_df["op_statusEffectiveDate"]).year

us_date_df = us_date_df.rename(columns={"op_status": "Certification Status"})
    
# Further aggregate by month
us_date_df["op_statusEffectiveDate"] = us_date_df["op_statusEffectiveDate"].apply(lambda x: x.replace(day=1))

us_date_df = us_date_df.loc[us_date_df["year"] > datetime.datetime.now().year-10
    ].groupby(["Certification Status","op_statusEffectiveDate"], as_index=False
    )["op_count"].sum()

sns.set_style("whitegrid")
sns.set_palette("Set1")
sns.set_context("poster")
sns.relplot(data=us_date_df, x="op_statusEffectiveDate", y="op_count", kind="line", hue="Certification Status",height=14, aspect=1.5, linewidth=3)

#plt.title("U.S. Certification Changes Over Time")
plt.xlabel("Date")
plt.ylabel("Monthly Change")

# From https://stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
us_date_url = "static\\images\\us_certification_date.png"
plt.savefig(us_date_url, bbox_inches="tight", pad_inches=0.3)


# us_certification_date_basic.png---------------------------
# Create basic certification change plot - certified minus surrendered, suspended, etc..
trend_df = pd.DataFrame(us_date)

# Get current count of US certified operations
us_cert_current = sum(trend_df.loc[trend_df["op_status"]=="Certified","op_count"])

# Function to apply in gettingmonthly certification count
def trend_change(trend):
    global us_cert_current
    us_cert_current = us_cert_current - trend
    return us_cert_current

trend_df['year'] = pd.DatetimeIndex(trend_df['op_statusEffectiveDate']).year
trend_df['op_statusEffectiveDate'] = trend_df['op_statusEffectiveDate'].apply(lambda x: x.replace(day=1))
trend_df = trend_df.loc[trend_df['year'] > 2018]
trend_df = trend_df.loc[trend_df["op_status"] != "Applied; APEDA Certified"]

trend_df = trend_df.groupby(['op_status','op_statusEffectiveDate'], as_index=False)['op_count'].sum()

trend_df = trend_df.pivot_table(
    index="op_statusEffectiveDate", 
    columns="op_status", 
    values="op_count",
    aggfunc="sum", fill_value=0,
    margins=False)

trend_df["Trend"] = trend_df["Certified"] - (trend_df["Revoked"] + trend_df["Surrendered"] + trend_df["Suspended"])

# Get total count of certified operations estimate per month
# This will not necessarily capture changes to/from suspension accurately
trend_df.sort_values("op_statusEffectiveDate", ascending=False, inplace=True)
trend_df["total_count"] = trend_df["Trend"].apply(trend_change)
trend_df.sort_values("op_statusEffectiveDate", ascending=True, inplace=True)
sns.set_style('whitegrid')
sns.set_palette('dark')
sns.set_context("poster")
sns.relplot(data=trend_df, x='op_statusEffectiveDate', y='Trend', kind='line',height=10, aspect=1.5)

#plt.title('Certification Changes Over Time')
plt.xlabel('Date')
plt.ylabel('Monthly Change')
plt.xticks(rotation=30)

# From https://stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
us_simple_status_date_url = "static\\images\\us_certification_date_basic.png"
plt.savefig(us_simple_status_date_url, bbox_inches="tight", pad_inches=0.3)

# us_certification_count.png-------------
# Plot total count per month using calculations in us_certification_date_basic.png section
sns.set_style('whitegrid')
sns.set_palette('flare')
sns.set_context("poster")
sns.relplot(data=trend_df, x='op_statusEffectiveDate', y='total_count', kind='line',height=10, aspect=1.5)

plt.xlabel('Date')
plt.ylabel('Certified Operations')
plt.xticks(rotation=30)

us_certification_count_url = "static\\images\\us_certification_count.png"
plt.savefig(us_certification_count_url, bbox_inches="tight", pad_inches=0.3)
