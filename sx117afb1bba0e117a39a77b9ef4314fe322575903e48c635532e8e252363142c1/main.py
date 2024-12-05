# -*- coding: utf-8 -*-
# Author: Longwe M.J.

# *********************************************************************************************
# ----------------------- *** Notes for future contract developers *** ------------------------
# 'contract_response_code' is for DApp usage, not nodes, used to react to the results. Nodes only use 'status' to know a contracts execution status.
# Never commit changes inside of a contract, the node will commit changes at finalisation for you else you run the risk of locking up your future contract executions with a wrong hash. Therefore, make sure you run the same database/storage as the entry node being used on Kujua. In this case we use the default MySql.
# *********************************************************************************************


# *********************************************************************************************
# ----------------------------------- *** Core Settings *** -----------------------------------
# *********************************************************************************************

version_no = "1.1.0"  # Version of this contract
version_target = "1.0.1"  # The minimum Kujua version this contract supports
about = "This contract is used for the facilitation of movements of value from sender to receiver through the use of perpetual contracts."

contract_address = "sx117afb1bba0e117a39a77b9ef4314fe322575903e48c635532e8e252363142c1"  # Add your public address here
contract_fee_address = contract_address  # Add your public address here to receive transactions should it be different to the contract address. Leave this to be the same as the contract_address if you intend to transact out funds received in contract_address but using a 0x address. Kujua allows contracts to transact under a 0x address prefix. In some cases the contract developer may need funds to be sent elsewhere rather than the contract address but it is up to the developer to itegrate the logic for this.
close_of_day_save = False  # This is what tells the blockchain to save the results hash in case you will need the results in future e.g typically the following day. At an extra fee.
requisites = {
    "save": 0,
    "contract_lock_status": 1,  # 0: unlocked funds, 1: keep funds locked - this prevents moving the assets until the contract logic is met
    "current_15m": {"stakers_fee_rate": 0.001, "withdrawal_rate": 0.001},
    "previous_15m": {"stakers_fee_rate": 0.001, "withdrawal_rate": 0.001},
}  # Requirements that can verify that a contract execution will be successful or not, prior to when the execution occurs e.g. a fee charge required to execute the contract may need to be known prior to the end user executing the contract so that they do not spend any blockchain fee only for their contract fee to be declined by the smart contract. These values are saved by the node every 15 minutes if the 'save' parameter is set to 1. Only the parameter 'save' and 'contract_lock_status' are standard, the rest are only relevant to this specific contract

# *********************************************************************************************
# --------------------------------- *** end of core settings *** ------------------------------
# *********************************************************************************************

import sys as sys_jua
sys_jua.path.append('')  # Location of custom modules such as "storage"

import storage  # storage is a custom module. Because Kujua does not allow custom modules and importing modules within the contract, one needs to run their own node when their custom module is a necessity in their contract. In this case we run this contract on our own node and distribute the results to the blockchain. This is because the blockchain does not store custom database data but only stores the most recent state of the contract. The contract will post this state on a periodic basis. Luckily running a node on Kujua is very cheap and easy and can run on a 4gb ram computer depending on how busy your contract gets.
from contract.contract import Contract
from contract.definitions import Definitions
from cron.close_of_day import CloseOfDay
import kujuaRelayChain.Client.functions as functions
from db_config import (
    HistoryLatest,
)  # Kujua allows accessing standard tables such as History as read-only as this is where transactional data is stored. In this case we use from... as we are plugging in directly to the nodes read-only configuration.
import string as string_jua
import random as random_jua
import datetime as datetime_jua
import ast as ast_jua
import hashlib as hashlib_jua
import sqlalchemy as sqlalchemy_jua

contract = Contract(
    contract_address=contract_address,
    contract_fee_address=contract_fee_address,
    close_of_day_save=close_of_day_save,
    requisites=requisites,
)

closeofday = CloseOfDay(
    contract_address=contract_address,
    contract_fee_address=contract_fee_address,
    close_of_day_save=close_of_day_save,
    requisites=requisites,
)
