# -*- coding: utf-8 -*-
# Author: Longwe M.J.

import sys as sys_jua
sys_jua.path.append('')  # Location of custom modules such as "storage"
from contract.definitions import Definitions
import storage  # storage is a custom module. Because Kujua does not allow custom modules and importing modules within the contract, one needs to run their own node when their custom module is a necessity in their contract. In this case we run this contract on our own node and distribute the results to the blockchain. This is because the blockchain does not store custom database data but only stores the most recent state of the contract. The contract will post this state on a periodic basis. Luckily running a node on Kujua is very cheap and easy and can run on a 4gb ram computer depending on how busy your contract gets.
from storage import db
import string as string_jua
import random as random_jua
import datetime as datetime_jua
import ast as ast_jua
import hashlib as hashlib_jua
import sqlalchemy as sqlalchemy_jua

# Optional: processes (cron jobs) to run upon the close of day, see whitepaper for details on what the close of day is
class CloseOfDay(Definitions):
    def __int__(
        self,
        contract_address,
        contract_fee_address,
        close_of_day_save,
        requisites,
    ):
        super().__init__(
            contract_address,
            contract_fee_address,
            close_of_day_save,
            requisites,
        )

    def withdrawals(self, current_gateway_decoded: str, _day: str, quoted_price: float):
        # Process withdrawal orders for the day

        # We do not filter by date as there may be orders from the previous days to process first
        withdraw_orders = (
            storage.db.session.query(
                storage.WithdrawalOrders.owner_address,
                storage.WithdrawalOrders.order_id,
                storage.WithdrawalOrders.fill_amount,
                storage.WithdrawalOrders.amount,
                storage.WithdrawalOrders.asset_peg,
                storage.WithdrawalOrders.packet_id,
            )
            .filter(storage.WithdrawalOrders.status == 1)
            .order_by(storage.WithdrawalOrders.recieved_datetime_id.asc())
            .all()
        )
        if withdraw_orders != []:
            # Check if there is enough reserve to withdraw above the 100% collateralisation level
            total_value_locked__ = storage.db.session.query(
                storage.Config.total_reserves_amount,
                storage.Config.total_min_operational_value,
                storage.Config.asset_peg,
            ).all()

            withdrawable_assets = {}  # Format: {asset: amount}
            total_min_operational_values = {}
            for asset_val in total_value_locked__:
                if float(asset_val[1]) > (float(asset_val[0]) * quoted_price):
                    withdrawable_assets[asset_val[2]] = float(asset_val[1]) - (
                        float(asset_val[0]) * quoted_price
                    )
                    total_min_operational_values[asset_val[2]] = float(asset_val[1])

            if withdrawable_assets == {}:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "contract_response_hash": str(),
                    "content": "All asset reserves below operational levels to allow withdrawals, waiting for more reserves to be added before processing withdrawals.",
                    "requisites": {},
                }

            withdrawal_orders = {}  # Format: {packet_id: [amount, asset]}
            withdrawal_asset_value = {}  # Format: {asset: amount}
            sequential_hashes = []
            for order in withdraw_orders:
                # Notice we allow a 30% extra leeway of reserves to keep the 100% collaterisation from falling below if market prices change
                if (
                    withdrawable_assets[order[4]]
                    - (float(total_min_operational_values[order[4]]) * 0.3)
                ) >= (float(order[3]) * quoted_price):
                    if order[4] not in iter(withdrawal_asset_value.keys()):
                        withdrawal_asset_value[order[4]] = 0

                    withdrawal_asset_value[order[4]] += (float(order[3])) * quoted_price

                    if order[5] not in iter(withdrawal_orders.keys()):
                        withdrawal_orders[order[5]] = [0, order[4]]
                        sequential_hashes.append(order[5])

                    withdrawal_orders[order[5]][0] += float(
                        order[3]
                    )  # Sums all transaction_type 5's from the same packet together before processing
                    withdrawable_assets[order[4]] -= float(order[3])

            # Notice we do not update the accumulation tables as they would have been updated upon placing the orders

            # Disable the transactions on the smart contract
            obj_count_______________ = 0
            loop_obj_______________ = withdrawal_orders[:]
            for req in withdrawal_orders:
                loop_obj_______________[obj_count_______________] = (
                    storage.db.session.query(storage.WithdrawalOrders)
                    .filter(storage.WithdrawalOrders.packet_id == req[0])
                    .first()
                )
                loop_obj_______________[obj_count_______________].status = -1
                obj_count_______________ += 1

            update_based_changes__ = db.session.query(storage.WithdrawalOrders.status).order_by(storage.WithdrawalOrders.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
            self.response_hash += (update_based_changes__,)

            # save the response hash
            contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

            return {
                "status": "complete",
                "contract_response_code": 1 if (sequential_hashes != []) else 0,
                "contract_response_hash": contract_response_hash,  # if (sequential_hashes != []) else '',
                "content": "Previously delayed withdrawals have been processed for the day {} for the available excess reserves.".format(
                    _day
                ),
                "requisites": {
                    "save": 1 if (sequential_hashes != []) else 0,
                    "contract_unlock_hashes": sequential_hashes,
                },
            }
        else:
            # save the response hash
            contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

            return {
                "status": "complete",
                "contract_response_code": 1,
                "contract_response_hash": contract_response_hash,
                "content": "No withdrawal orders were placed for the day: {}".format(
                    _day
                ),
                "requisites": {},
            }

    def results(self, current_gateway_decoded: str, _day: str, quoted_price: float):
        withdrawals = self.withdrawals(current_gateway_decoded, _day, quoted_price)
        if withdrawals["status"] == "complete":
            return withdrawals
        else:
            # save the response hash
            contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

            return {
                "status": "complete",
                "contract_response_code": 1,
                "contract_response_hash": contract_response_hash,
                "content": "Close of day contract process finished with response: {}".format(
                    withdrawals["content"]
                ),
                "requisites": {},
            }
