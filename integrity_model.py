from flask_sqlalchemy import SQLAlchemy
#from flask_migrate import Migrate
#from flask import Flask, redirect, render_template, request, url_for
#from flask_login import login_user, LoginManager, UserMixin, logout_user, login_required, current_user
#from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime
from mysql import connector

db = SQLAlchemy()

#class User(UserMixin, db.Model):
#    __tablename__ = 'user'
#    user_id = db.Column(db.Integer, primary_key=True)
#    user_name = db.Column(db.String(128))
#    password_hash = db.Column(db.String(128))

#    def check_password(self, password):
#        return check_password_hash(self.password_hash, password)

#    def get_id(self):
#        return self.user_name


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
    ci_nopOpID = db.Column(db.String(256))
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
    nopOpID_constraint = db.relationship("OrganicOperation", foreign_keys=ci_nopOpID)

