from flask import Flask, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from integrity_app import app
from integrity_app.integrity_model import db, OrganicOperation, OrganicItem
import pandas as pd
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
from os import sep
from sqlalchemy.sql.functions import coalesce

# Set file path variables
static = "integrity_app" + sep + "integrity_app/static" + sep
static_img = "integrity_app" + sep + "integrity_app/static" + sep + "images" + sep

# Set plot parameters
plt.rcParams.update({"font.size": 22})

#----------------World page route-------------------------
@app.route("/world_process", methods=["GET", "POST"])
def world_process():
    if request.method != "POST":
        return "POST to re-process data for World view"
    else:
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

        country_table.to_csv("integrity_app/static/op_status_country.csv", index=False)

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

        # From https:/stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
        plt.savefig("integrity_app/static/images/certification_date.png", bbox_inches="tight", pad_inches=0.3)


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

        # From https:/stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
        plt.savefig("integrity_app/static/images/certification_date_basic.png", bbox_inches="tight", pad_inches=0.3)

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

        pd.DataFrame(scopes_count, index=[0]).to_csv("integrity_app/static/scopes_count.csv", index=False)

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

        scope_set.to_csv("integrity_app/static/scopes_combo.csv", index=False)

        return "The world view data was processed!"


#------------------------------united_states page route-------------------------------
@app.route("/us_process", methods=["GET", "POST"])
def us_process():
    if request.method != "POST":
        return "POST to re-process data for U.S. view"
    else:
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

        us_pivot.to_csv("integrity_app/static/us_table.csv", index=False)

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

        us_scopes_return.to_csv("integrity_app/static/us_scopes_return.csv", index=False)

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

        plt.savefig("integrity_app/static/images/us_certification_date.png", bbox_inches="tight", pad_inches=0.3)


        # us_certification_date_basic.png---------------------------
        # Create basic certification change plot - certified minus surrendered, suspended, etc..
        trend_df = pd.DataFrame(us_date)

        # Get current count of US certified operations
        global us_cert_current
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

        plt.savefig("integrity_app/static/images/us_certification_date_basic.png", bbox_inches="tight", pad_inches=0.3)

        # us_certification_count.png-------------
        # Plot total count per month using calculations in us_certification_date_basic.png section
        sns.set_style('whitegrid')
        sns.set_palette('flare')
        sns.set_context("poster")
        sns.relplot(data=trend_df, x='op_statusEffectiveDate', y='total_count', kind='line',height=10, aspect=1.5)

        plt.xlabel('Date')
        plt.ylabel('Certified Operations')
        plt.xticks(rotation=30)

        plt.savefig("integrity_app/static/images/us_certification_count.png", bbox_inches="tight", pad_inches=0.3)

        return "The United States view data was processed!"


#----------------Products page route-------------------------
@app.route("/products_process", methods=["GET", "POST"])
def products_process():
    if request.method != "POST":
        return "POST to re-process data for World view"
    else:
        country_items = db.session.query(
        coalesce(OrganicItem.ci_nopCatName,
        OrganicItem.ci_itemList,
        OrganicItem.ci_nopCategory),
        OrganicItem.ci_nopScope,
        OrganicOperation.opPA_country
        ).select_from(OrganicItem).join(OrganicOperation).filter(
        OrganicItem.ci_status == "Certified").all()

        items = pd.DataFrame(country_items, columns=["Items","Scope","Country"])
        items = items.loc[~items["Items"].isna()]

        # Replace ampersand with comma
        items["Items"] = items["Items"].str.replace("&",",",False)

        # Convert to list of lists for iterating.
        items = items.values.tolist()

        # For each entry, separate on commas and add a new row (or nested list) for each separated entry
        item_new = list()

        for item in items:
            temp = item[0].split(",")
            for entry in temp:
                item_new.append([entry,item[1], item[2]])

        items = pd.DataFrame(item_new)

        # Replace anything inside quotes. These are most likely brand names.
        items[0] = items[0].str.replace("[\"|\“|\”].*[\"|\“|\”]|[\"|\“|\”]","",regex=True)
        
        # Trim the items
        items[0] = items[0].str.strip()
        # Convert casing
        items[0] = items[0].str.lower()
        # Get rid of empty rows or rows consisting of some known adjectives.
        bad_items = ["",
        "natural",
        "dried",
        "packaging",
        "processing",
        "vegetables",
        "vegetable",
        "fruits",
        "fruit",
        "herbs",
        "herb",
        "oils"]
        items = items.loc[~(items[0].isin(bad_items))]

        # Aggregate
        item_count = items.groupby([0,1], as_index=False).size().sort_values("size",ascending=False)

        item_count_c = item_count.loc[item_count[1] == "Crops"]
        item_count_l = item_count.loc[item_count[1] == "Livestock"]
        item_count_h = item_count.loc[item_count[1] == "Handling"]
        item_count_w = item_count.loc[item_count[1] == "Wild Crops"]

        item_top = pd.concat([item_count_c[0:10], item_count_l[0:10], item_count_h[0:10], item_count_w[0:10]]).reset_index()

        item_count_c.iloc[0:10,[0,2]].to_csv("top_items_crops.csv",index=False)
        item_count_l.iloc[0:10,[0,2]].to_csv("top_items_livestock.csv",index=False)
        item_count_h.iloc[0:10,[0,2]].to_csv("top_items_handling.csv",index=False)
        item_count_w.iloc[0:10,[0,2]].to_csv("top_items_wild.csv",index=False)
        
        return "The products view data was processed!"
