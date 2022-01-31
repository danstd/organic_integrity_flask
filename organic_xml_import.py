import xml.etree.ElementTree as ET
import pandas as pd
from mysql import connector
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime
import sys
from collections import OrderedDict


# This function extracts from xml and returns a dataframe.
# Assumes no elements past the first two levels are nested, as matches the specifications.
def xml_to_pd(xml_data, subroot_name, col_list):
    outer_list=list()
    for entry in xml_data.findall(subroot_name):
        inner_list=list()
        for col in col_list:
            try:
                inner_list.append(entry.find(col).text)
            except AttributeError:
                inner_list.append(None)
        outer_list.append(inner_list)
    return pd.DataFrame(data = outer_list, columns = col_list)


# Read in the xml file.
tree = ET.parse("integrity_download\\stream.xml")

root = tree.getroot()

# Set the operation-level column names.
op_cols = OrderedDict()
op_cols['op_certifierName'] = String()
op_cols['op_nopOpID'] = String()
op_cols['op_name'] = String()
op_cols['op_otherNames'] = String()
op_cols['op_clientID'] = String()
op_cols['op_contFirstName'] = String()
op_cols['op_contLastName'] = String()
op_cols['op_status'] = String()
op_cols['op_statusEffectiveDate'] = DateTime()
op_cols['op_nopAnniversaryDate'] = DateTime()
op_cols['op_lastUpdatedDate'] = DateTime()
op_cols['opSC_CR'] = String()
op_cols['opSC_CR_ED'] = DateTime()
op_cols['opSC_LS'] = String()
op_cols['opSC_LS_ED'] = DateTime()
op_cols['opSC_WC'] = String()
op_cols['opSC_WC_ED'] = DateTime()
op_cols['opSC_HANDLING'] = String()
op_cols['opSC_HANDLING_ED'] = DateTime()
op_cols['opPA_line1'] = String()
op_cols['opPA_line2'] = String()
op_cols['opPA_city'] = String()
op_cols['opPA_state'] = String()
op_cols['opPA_country'] = String()
op_cols['opPA_zip'] = String()
op_cols['opPA_countyCode'] = String()
op_cols['opPA_county'] = String()
op_cols['opMA_line1'] = String()
op_cols['opMA_line2'] = String()
op_cols['opMA_city'] = String()
op_cols['opMA_state'] = String()
op_cols['opMA_country'] = String()
op_cols['opMA_zip'] = String()
op_cols['opMA_countyCode'] = String()
op_cols['opMA_county'] = String()
op_cols['op_phone'] = String()
op_cols['op_email'] = String()
op_cols['op_url'] = String()
#op_cols['op_opExtraInfo'] = String() #This column has too much extraneous information.
op_cols['opEx_broker'] = String()
op_cols['opEx_csa'] = String()
op_cols['opEx_copacker'] = String()
op_cols['opEx_dairy'] = String()
op_cols['opEx_distributor'] = String()
op_cols['opEx_marketerTrader'] = String()
op_cols['opEx_restaurant'] = String()
op_cols['opEx_retailer'] = String()
op_cols['opEx_poultry'] = String()
op_cols['opEx_privateLabeler'] = String()
op_cols['opEx_slaughterHouse'] = String()
op_cols['opEx_storage'] = String()
op_cols['opEx_growerGroup'] = String()
op_cols['opCert_url'] = String()

# Set the item-lvel column names.
item_cols = [
'ci_nopOpID',
'ci_certNumber',
'ci_nopScope',
'ci_nopCategory',
'ci_nopCatID',
'ci_nopCatName',
'ci_nopItemID',
'ci_itemList',
'ci_varieties',
'ci_status',
'ci_statusEffectiveDate',
'ci_organic100',
'ci_organic',
'ci_madeWithOrganic'
]

# Extract the operation-level data.
operations = xml_to_pd(root[0], "Operation", op_cols.keys())

# Extract the item level data.
items = xml_to_pd(root[1], "Item", item_cols)

# Check the results are of the expected length.
# The operation-level data specifically should only go up over time.

if len(operations) < 68000 or len(items) < 460000:
    # Set up a log for tracking.
    print("XML did not contain expected data!")
    sys.exit()

SQLALCHEMY_DATABASE_URI = "mysql+mysqlconnector://{username}:{password}@{hostname}/{databasename}".format(
    username="integrity_update",
    password="dersAGef3rover",
    hostname="127.0.0.1",
    databasename="organic_integrity",
)


engine = create_engine(SQLALCHEMY_DATABASE_URI)

operations.to_sql(name="organic_operations", con=engine, schema="organic_integrity", if_exists="replace", index=False)
items.to_sql(name="organic_items", con=engine, schema="organic_integrity", if_exists="replace", index=False)

#items.to_csv("organic_items.csv", index=False)
#operations.to_csv("organic_operations.csv", index=False)

