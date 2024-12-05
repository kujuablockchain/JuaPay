from flask_sqlalchemy import SQLAlchemy
from os import environ
import pymysql
from sqlalchemy.dialects.mysql import LONGTEXT, BIGINT, NUMERIC
from flask_api import FlaskAPI
import sys
# if you experience error: KeyError: 'SQLALCHEMY_TRACK_MODIFICATIONS' with SQLAlchemy, use "pip3 install Flask-SQLAlchemy==2.1"

class ConfigMode():
    try:
        kujua_database_user = environ["kujua_database_user"]
        kujua_login_password = environ["kujua_login_password"]
        kujua_hostname = environ["kujua_hostname"]
        database = environ["database"]
        juapay_database = environ["juapay_database"]
        status = True
    except KeyError:
        status = None  # None is needed by the main app
        kujua_database_user = None
        kujua_login_password = None
        kujua_hostname = None
        database = None
        juapay_database = None

        raise Exception('Complete setting up the settings file before running the node. See documentation.')

# from custom_modules.settings import server_path
# sys.path.append(server_path)

config_cls = ConfigMode()

app = FlaskAPI(__name__)  # , template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)  # Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "mysql+pymysql://"+ config_cls.kujua_database_user + ":" + config_cls.kujua_login_password + "@" + config_cls.kujua_hostname + "/" + config_cls.juapay_database
app.config['SQLALCHEMY_BINDS'] = {config_cls.database: "mysql+pymysql://"+ config_cls.kujua_database_user + ":" + config_cls.kujua_login_password + "@" + config_cls.kujua_hostname + "/" + config_cls.database,
                                  config_cls.juapay_database: "mysql+pymysql://"+ config_cls.kujua_database_user + ":" + config_cls.kujua_login_password + "@" + config_cls.kujua_hostname + "/" + config_cls.juapay_database}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)

class Config(db.Model):
    __table_args__ = {'extend_existing': True}  # allows rewriting the table if there is a name clash. Useful when resetting up the project
    __bind_key__ = config_cls.juapay_database

    id = db.Column(db.BIGINT, primary_key=True)
    asset_peg = db.Column(db.String(10), nullable=False, unique=True)
    withdrawal_datetime_id = db.Column(db.String(20), nullable=False)  # The last time a withdrawal job ran
    total_contracted_amount = db.Column(db.NUMERIC(34, 4), nullable=False)
    total_min_operational_value = db.Column(db.NUMERIC(34, 2), nullable=False)  # The 100% collaterilised converted value locked in asset peg terms as at the time the reserves were locked. It is the quantity of Juana multiplied by the then quoted price for that asset. This is used to compare to the current level of reserves
    total_reserves_amount = db.Column(db.NUMERIC(34, 4), nullable=False)
    fee_rate = db.Column(db.NUMERIC(10, 4), nullable=False)
    updated = db.Column(db.DateTime, nullable=False)

    def __init__(self, asset_peg, withdrawal_datetime_id, total_contracted_amount, total_min_operational_value, total_reserves_amount, fee_rate, updated):

        self.asset_peg = asset_peg
        self.withdrawal_datetime_id = withdrawal_datetime_id
        self.total_contracted_amount = total_contracted_amount
        self.total_min_operational_value = total_min_operational_value
        self.total_reserves_amount = total_reserves_amount
        self.fee_rate = fee_rate
        self.updated = updated

    def __repr__(self):
        return self.asset_peg, self.withdrawal_datetime_id, self.total_contracted_amount, self.total_min_operational_value, self.total_reserves_amount, self.fee_rate, self.updated

class Contracts(db.Model):
    __table_args__ = {'extend_existing': True}  # allows rewriting the table if there is a name clash. Useful when resetting up the project
    __bind_key__ = config_cls.juapay_database

    id = db.Column(db.BIGINT, primary_key=True)
    claimed_packet_ids = db.Column(LONGTEXT, nullable=False)  # List of used reserves to handle this contract addition
    owner_address = db.Column(db.String(255), nullable=False)
    hash = db.Column(db.String(255), nullable=False)
    asset = db.Column(db.String(255), nullable=False)
    issuer_address = db.Column(db.String(255), nullable=False)
    asset_peg = db.Column(db.String(10), nullable=False)
    value = db.Column(db.NUMERIC(34, 2), nullable=False)  # This is the 1:1 pegged value, also what is shown to the user i.e the moving balance
    lock_period = db.Column(db.Integer, nullable=False)  # In days
    lock_ratio = db.Column(db.Integer, nullable=False)
    amount_lock = db.Column(db.NUMERIC(34, 4), nullable=False)  # The Juana equivalent of value. Calculated as value / quoted price at initial issue. It uses the initial quoted price instead of current quoted price so as to be able to divide the quantity equally in future when contracts are transacted.
    transaction_value = db.Column(db.NUMERIC(34, 4), nullable=False)  # The asset_peg amount as entered by the user  delete--> Uses 4 decimals as its a mix of value and amount in juana and local asset_peg
    stakers_fee = db.Column(db.NUMERIC(34, 4), nullable=False)
    utc_time = db.Column(db.NUMERIC(20), nullable=False)
    receipt_date_id = db.Column(db.Integer, nullable=False)
    type = db.Column(db.String(30), nullable=False)
    contract = db.Column(LONGTEXT, nullable=False)  # Used to compare the contract sent and the contract recieved with all its changes.
    status = db.Column(db.Integer, nullable=False)
    packet_id = db.Column(db.String(255), nullable=False)
    quote_price = db.Column(db.NUMERIC(34, 2), nullable=False)  # The Juana price when the contract was created and passed down as is if the contract splits, quoted price at initial issue. I.e. the initial quoted price instead of current quoted price so as to be able to divide the quantity equally in future when contracts are transacted. This is also used to know whether the total locked value reserves are still enough to cover all contracts.
    updated = db.Column(db.DateTime, nullable=False)

    def __init__(self, claimed_packet_ids, owner_address, hash, asset, issuer_address, asset_peg, value, lock_period, lock_ratio, amount_lock, transaction_value, stakers_fee, utc_time, receipt_date_id, type, contract, status, packet_id, quote_price, updated):

        self.claimed_packet_ids = claimed_packet_ids
        self.owner_address = owner_address
        self.hash = hash
        self.asset = asset
        self.issuer_address = issuer_address
        self.asset_peg = asset_peg
        self.value = value
        self.lock_period = lock_period
        self.lock_ratio = lock_ratio
        self.amount_lock = amount_lock
        self.transaction_value = transaction_value
        self.stakers_fee = stakers_fee
        self.utc_time = utc_time
        self.receipt_date_id = receipt_date_id
        self.type = type
        self.contract = contract
        self.status = status
        self.packet_id = packet_id
        self.quote_price = quote_price
        self.updated = updated

    def __repr__(self):
        return self.claimed_packet_ids, self.owner_address, self.hash, self.asset, self.issuer_address, self.asset_peg, self.value, self.lock_period, self.lock_ratio, self.amount_lock, self.transaction_value, self.stakers_fee, self.utc_time, self.receipt_date_id, self.type, self.contract, self.status, self.packet_id, self.created

class ReserveTransactions(db.Model):
    __table_args__ = {'extend_existing': True}  # allows rewriting the table if there is a name clash. Useful when resetting up the project
    __bind_key__ = config_cls.juapay_database

    id = db.Column(db.BIGINT, primary_key=True)
    transaction_type = db.Column(db.Integer, nullable=False)
    coin = db.Column(LONGTEXT, nullable=False)
    sent_date_id = db.Column(db.NUMERIC(20), nullable=False)
    issuer_address = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Integer, nullable=False)
    packet_id = db.Column(db.String(255), nullable=False)
    asset_peg = db.Column(db.String(10), nullable=False)
    amount = db.Column(db.NUMERIC(34, 4), nullable=False)
    transaction_value = db.Column(db.NUMERIC(34, 4), nullable=False)
    lock_ratio = db.Column(db.Integer, nullable=False)
    type = db.Column(db.String(30), nullable=False)
    quote_price = db.Column(db.NUMERIC(34, 2), nullable=False)  # The Juana price when the reserve was created, this is used to know whether the total locked value reserves are still enough to cover all contracts
    updated = db.Column(db.DateTime, nullable=False)

    def __init__(self, transaction_type, coin, sent_date_id, issuer_address, status, packet_id, asset_peg, amount, transaction_value, lock_ratio, type, quote_price, updated):

        self.transaction_type = transaction_type
        self.coin = coin
        self.sent_date_id = sent_date_id
        self.issuer_address = issuer_address
        self.status = status
        self.packet_id = packet_id
        self.asset_peg = asset_peg
        self.amount = amount
        self.transaction_value = transaction_value
        self.lock_ratio = lock_ratio
        self.type = type
        self.quote_price = quote_price
        self.updated = updated

    def __repr__(self):
        return self.transaction_type, self.coin, self.sent_date_id, self.issuer_address, self.status, self.packet_id, self.amount, self.transaction_value, self.lock_ratio, self.type, self.created

# Sum all reserves total value for ReserveTransactions in TotalValueLocked
class TotalValueLocked(db.Model):
    __table_args__ = {'extend_existing': True}  # allows rewriting the table if there is a name clash. Useful when resetting up the project
    __bind_key__ = config_cls.juapay_database

    id = db.Column(db.BIGINT, primary_key=True)
    asset_peg = db.Column(db.String(10), nullable=False)
    lock_ratio = db.Column(db.Integer, nullable=False)
    total_contracted_amount = db.Column(db.NUMERIC(34, 4), nullable=False)  # The total amount of Juana held in contracts
    total_reserves_amount = db.Column(db.NUMERIC(34, 4), nullable=False)  # The total amount of Juana reserves remaining to be used
    min_operational_value = db.Column(db.NUMERIC(34, 2), nullable=False)  # The 100% collaterilised converted value locked in asset peg terms as at the time the reserves were locked. It is the quantity of Juana multiplied by the then quoted price for that asset. This is used to compare to the current level of reserves
    updated = db.Column(db.DateTime, nullable=False)

    def __init__(self, asset_peg, lock_ratio, total_contracted_amount, total_reserves_amount, min_operational_value, updated):

        # self.date_id = date_id
        self.asset_peg = asset_peg
        self.lock_ratio = lock_ratio
        self.total_contracted_amount = total_contracted_amount
        self.total_reserves_amount = total_reserves_amount
        self.min_operational_value = min_operational_value
        self.updated = updated

    def __repr__(self):
        return self.asset_peg, self.lock_ratio, self.total_contracted_amount, self.total_reserves_amount, self.min_operational_value, self.updated

class WithdrawalOrders(db.Model):
    __table_args__ = {'extend_existing': True}  # allows rewriting the table if there is a name clash. Useful when resetting up the project
    __bind_key__ = config_cls.juapay_database

    id = db.Column(db.BIGINT, primary_key=True)
    owner_address = db.Column(db.String(255), nullable=False)
    order_id = db.Column(db.String(255), nullable=False, unique=True)
    date_id = db.Column(db.NUMERIC(20), nullable=False)  # The contract generated date when this order was created
    process_date_id = db.Column(db.NUMERIC(20), nullable=False)  # When to execute this order
    datetime_id = db.Column(db.NUMERIC(20), nullable=False)  # The contract generated time when this order was created
    coin = db.Column(LONGTEXT, nullable=False)
    recieved_datetime_id = db.Column(db.NUMERIC(20), nullable=False)  # The parent node's process time
    amount = db.Column(db.NUMERIC(34, 4), nullable=False)
    fill_amount = db.Column(db.NUMERIC(34, 4), nullable=False)  # The amount filled so long
    status = db.Column(db.Integer, nullable=False)  # -1: inactive (paid and confirmed), 1: active / queued (unpaid)
    asset_peg = db.Column(db.String(10), nullable=False)
    packet_id = db.Column(db.String(255), nullable=False, unique=True)
    updated = db.Column(db.DateTime, nullable=False)

    def __init__(self, owner_address, order_id, date_id, process_date_id, datetime_id, coin, recieved_datetime_id, amount, fill_amount, status, asset_peg, packet_id, updated):

        self.owner_address = owner_address
        self.order_id = order_id
        self.date_id = date_id
        self.process_date_id = process_date_id
        self.datetime_id = datetime_id
        self.coin = coin
        self.recieved_datetime_id = recieved_datetime_id
        self.amount = amount
        self.fill_amount = fill_amount
        self.status = status
        self.asset_peg = asset_peg
        self.packet_id = packet_id
        self.updated = updated

    def __repr__(self):
        return self.owner_address, self.order_id, self.date_id, self.process_date_id, self.datetime_id, self.coin, self.recieved_datetime_id, self.amount, self.fill_amount, self.status, self.asset_peg, self.packet_id, self.updated
