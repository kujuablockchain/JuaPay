# -*- coding: utf-8 -*-
# Author: Longwe M.J.

import datetime as datetime_jua
import hashlib as hashlib_jua
import sqlalchemy as sqlalchemy_jua

import sys as sys_jua
sys_jua.path.append('')  # Location of custom modules such as "storage"
from contract.definitions import Definitions
import storage  # storage is a custom module. Because Kujua does not allow custom modules and importing modules within the contract, one needs to run their own node when their custom module is a necessity in their contract. In this case we run this contract on our own node and distribute the results to the blockchain. This is because the blockchain does not store custom database data but only stores the most recent state of the contract. The contract will post this state on a periodic basis. Luckily running a node on Kujua is very cheap and easy and can run on a 4gb ram computer depending on how busy your contract gets.
from storage import db

class Contract(Definitions):
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

    def results(
        self,
        packet: dict,
        packet_id: str,
        recieved_datetime_id: int,
        current_gateway_decoded: str,
        _day: str,
        quoted_price: float,
        increment: int,
        requisites: dict,
        contract_args: dict,
    ):
        try:
            check_exist = (
                storage.db.session.query(storage.TotalValueLocked.id)
                .filter(storage.TotalValueLocked.total_contracted_amount >= 0)
                .first()
            )

            # Add todays default values if its the first run for the day
            if check_exist == None:
                for cur in self.supported_currency_codes:
                    for lock_ratio in range(3, 21):
                        db_insert = storage.TotalValueLocked(
                            cur,
                            lock_ratio,
                            0,
                            0,
                            0,
                            datetime_jua.datetime.utcnow().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                        )

                        for x in range(3):
                            try:
                                storage.db.session.add(db_insert)
                                break
                            except:
                                storage.db.session.rollback()
                                if x == 2:
                                    raise Exception(
                                        "Could not access the database upon insert."
                                    )

                        self.response_hash += (cur,
                            lock_ratio,
                            0,
                            0,
                            0,)

                print("Table TotalValueLocked updated for the day")

            check_exist = (
                storage.db.session.query(storage.Config.id)
                .filter(storage.Config.id == 1)
                .first()
            )
            if check_exist == None:
                for cur in self.supported_currency_codes:
                    db_insert = storage.Config(
                        cur,
                        "19990101235959999999",
                        0,
                        0,
                        0,
                        0,
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (cur,
                            "19990101235959999999",
                            0,
                            "",
                            0,
                            0,
                            0,
                            0,)

        except sqlalchemy_jua.exc.ProgrammingError:
            storage.db.drop_all()
            storage.db.create_all()

            for cur in self.supported_currency_codes:
                for lock_ratio in range(3, 21):
                    db_insert = storage.TotalValueLocked(
                        cur,
                        lock_ratio,
                        0,
                        0,
                        0,
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (cur,
                                lock_ratio,
                                0,
                                0,
                                0,)

            for cur in self.supported_currency_codes:
                db_insert = storage.Config(
                    cur,
                    "19990101235959999999",
                    0,
                    0,
                    0,
                    0,
                    datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )

                for x in range(3):
                    try:
                        storage.db.session.add(db_insert)
                        break
                    except:
                        storage.db.session.rollback()
                        if x == 2:
                            raise Exception(
                                "Could not access the database upon insert."
                            )

                self.response_hash += (cur,
                        "19990101235959999999",
                        0,
                        "",
                        0,
                        0,
                        0,
                        0,)

            # save the response hash
            contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

            return {
                "status": "error",
                "contract_response_code": 0,
                "contract_response_hash": contract_response_hash,
                "content": "Tables created in database",
                "requisites": {},
            }

        # Send reserves contracts sent from one account to another (NOT JUANA). If transacting from one smart_contract account to another
        if contract_args["action"] == "transact_p2p":
            if contract_args != {}:
                issuer_lock_ratio_amounts = {}
                
                # Ask the node to save the requisites if it does not have any saved for this contract
                if ("current_15m" not in iter(requisites.keys())) or (
                    "previous_15m" not in iter(requisites.keys())
                ):
                    requisites = {
                        "save": 1,
                        "contract_lock_status": 1,
                        "current_15m": {
                            "stakers_fee_rate": 0.001,
                            "withdrawal_rate": 0.001,
                        },
                        "previous_15m": {
                            "stakers_fee_rate": 0.001,
                            "withdrawal_rate": 0.001,
                        },
                    }  # Requirements that can verify that a contract execution will be successful or not, prior to when the execution occurs e.g. a fee charge required to execute the contract may need to be known prior to the end user executing the contract so that they do not spend any blockchain fee only for their contract fee to be declined by the smart contract. These values are saved by the node every 15 minutes if the 'save' parameter is set to 1. Only the 'save' parameter is standard, the rest are only relevant to this specific contract

                    storage.db.session.rollback()
                    return {
                        "status": "complete",  # NB: when saving requisites, always use 'complete' as status so the node commits the changes else they will not
                        "contract_response_code": 1,
                        "contract_response_hash": str(),
                        "content": "Missing required parameter for requisites 'save', see documentation. Request to save parameter sent. Rerun last transaction.",
                        "requisites": requisites,
                    }

                # Get both the current and previous 15 minute's rates, if the current rate period has passed the contract will allow using the previous 15 minute's rate
                stakers_fee_rate_c = requisites["current_15m"]["stakers_fee_rate"]
                stakers_fee_rate_p = requisites["previous_15m"]["stakers_fee_rate"]

                if float(contract_args["contract"]["transaction_value"]) <= 0:
                    self.response_hash += (requisites,)

                    # save the response hash
                    contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": contract_response_hash,
                        "content": "Invalid amount transacted",
                        "requisites": {},
                    }

                elif (contract_args["contract"]["owner_address"][-4:] == '.jua') or ('.jua.' in contract_args["contract"]["owner_address"]):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "The juapay contract does not support UUID via the API interface.",
                        "requisites": {},
                    }

                elif (
                    contract_args["contract"]["owner_address"]
                    == list(packet.values())[0]["sender_address"]
                ):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Cannot send to oneself",
                        "requisites": {},
                    }

                # Verify they have valid contracts and add up to see that the total being sent is enough to send the transaction_value
                last_item = None
                contracts__ = (
                    storage.db.session.query(
                        storage.Contracts.claimed_packet_ids,
                        storage.Contracts.owner_address,
                        storage.Contracts.hash,
                        storage.Contracts.asset,
                        storage.Contracts.issuer_address,
                        storage.Contracts.asset_peg,
                        storage.Contracts.value,
                        storage.Contracts.lock_period,
                        storage.Contracts.lock_ratio,
                        storage.Contracts.amount_lock,
                        storage.Contracts.transaction_value,
                        storage.Contracts.utc_time,
                        storage.Contracts.receipt_date_id,
                        storage.Contracts.type,
                        storage.Contracts.contract,
                        storage.Contracts.status,
                        storage.Contracts.packet_id,
                        storage.Contracts.quote_price,
                    )
                    .filter(
                        storage.Contracts.owner_address
                        == list(packet.values())[0]["sender_address"],
                        storage.Contracts.status == 1,
                        storage.Contracts.asset_peg
                        == contract_args["contract"]["asset_peg"],
                        storage.Contracts.packet_id.in_(
                            contract_args["contract"]["claimed_packet_ids"]
                        ),
                    )
                    .order_by(storage.Contracts.value.desc()).all()  # We transact the largest contracts first, order is important for the fee share split calculations below
                )

                if contracts__ == []:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "No active contracts found for sender {} and asset peg {} on these sequential hashes: {}".format(
                            list(packet.values())[0]["sender_address"],
                            contract_args["contract"]["asset_peg"],
                            contract_args["contract"]["claimed_packet_ids"],
                        ),
                        "requisites": requisites,
                    }

                amount_lock_remainder_fill = 0
                disabled_sequential_hashes_list = []
                value_remainder_fill = 0
                value_fill = 0
                issuer_lock_ratio_amounts['amount_lock_remainder_fill'] = {}
                issuer_lock_ratio_amounts['value_remainder_fill'] = {}
                issuer_lock_ratio_amounts['value_fill'] = {}
                issuer_lock_ratio_amounts['quote_price'] = {}
                for req in contracts__:
                    # *** key checks ***
                    if req[4] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[4] not in iter(issuer_lock_ratio_amounts['value_remainder_fill'].keys()):
                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['value_remainder_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] = 0

                    if req[4] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[4] not in iter(issuer_lock_ratio_amounts['value_fill'].keys()):
                        issuer_lock_ratio_amounts['value_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['value_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] = 0
                        
                    if req[4] not in iter(issuer_lock_ratio_amounts['quote_price'].keys()):
                        issuer_lock_ratio_amounts['quote_price'][req[4]] = {}
                        issuer_lock_ratio_amounts['quote_price'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['quote_price'][req[4]].keys()):
                        issuer_lock_ratio_amounts['quote_price'][req[4]][req[8]] = 0

                    # *** end of key checks ***

                    disabled_sequential_hashes_list.append(req[16])
                    if (value_remainder_fill + float(req[6])) >= contract_args["contract"][
                        "transaction_value"
                    ]:
                        value_remainder_fill = (value_remainder_fill + float(req[6])) - contract_args[
                            "contract"
                        ]["transaction_value"]
                        amount_lock_remainder_fill = round(value_remainder_fill / float(req[17]), 4)  # the quote price is the same used at initial generation of the contract
                        value_fill += float(req[6]) - value_remainder_fill
                        last_item = req

                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] += float(req[6]) - value_remainder_fill
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] += round(issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] / float(req[17]), 4)  # the quote price is the same used at initial generation of the contract
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] += float(req[6]) - value_remainder_fill
                        issuer_lock_ratio_amounts['quote_price'][req[4]][req[8]] += req[17]

                        break

                    else:
                        value_remainder_fill += float(req[6])
                        amount_lock_remainder_fill += float(req[9])
                        value_fill += float(req[6])

                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] += float(req[6])
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] += float(req[9])
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] += float(req[6])

                if last_item == None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Unable to find required contract items",
                        "requisites": requisites,
                    }

                # It's possible that a contract is generated using more than a single reserve, therefore we may receiving sending split fees to multiple stakers so we lump them all up before checking if the total is adequate.
                expected_fee_share_split = {}
                for req in contracts__:
                    if req[16] == last_item[16]:
                        pass # The last item gets any remaining fee, we do not police how much is sent to them as we already check that an adequate fee is received. This means in cases of a single stakers reserves being used then we also do not check here as there is already an adequacy check below. This can prevent failures in processing transactions if a minute amount was sent extra due to a minor miscalculation from a dapp.
                    else:
                        if req[4] not in iter(expected_fee_share_split.keys()):
                            expected_fee_share_split[req[4]] = float(req[6])
                        else:
                            expected_fee_share_split[req[4]] += float(req[6])
                
                # It's possible that a contract is generated using more than a single reserve, therefore we may receiving sending split fees to multiple stakers so we lump them all up before checking if the total is adequate.
                fee_received = 0
                fee_received_service = 0
                received_fee_share_split = {}
                for f in iter(packet.values()):
                    if (f["transaction_type"] == "9"):
                        fee_received += float(f["note_balance"])

                        if f["receiver_address"] not in iter(received_fee_share_split.keys()):
                            received_fee_share_split[f["receiver_address"]] = float(f["note_balance"])
                        else:
                            received_fee_share_split[f["receiver_address"]] += float(f["note_balance"])

                    if f["receiver_address"] == self.contract_fee_address:
                        fee_received_service += float(f["note_balance"])

                # Convert service fee to JUA and check requirement, also prevents abuse of service
                if (fee_received_service == 0) or (fee_received_service < round(float(contract_args["contract"]["transaction_value"]) * 0.00001, 4)):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Insufficient service fee of {} JUA received, lower than calculated {} JUA at quoted price {} {}".format(fee_received_service, round(float(contract_args["contract"]["transaction_value"]) * 0.00001, 4), quoted_price, contract_args["contract"]["asset_peg"]),
                        "requisites": requisites,
                    }

                # Check if the sender sent enough contract value to split between the receiver and the sender's change at the current market price
                if value_fill < (
                    float(contract_args["contract"]["transaction_value"])
                ):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Insufficient contracts value receipt of {} {} to transact {} {}.".format(
                            value_fill,
                            contract_args["contract"]["asset_peg"],
                            contract_args["contract"]["transaction_value"],
                            contract_args["contract"]["asset_peg"],
                        ),
                        "requisites": {},
                    }

                if fee_received < (
                    float(contract_args["contract"]["transaction_value"])
                    * stakers_fee_rate_c
                ):
                    if fee_received < (
                        float(contract_args["contract"]["transaction_value"])
                        * stakers_fee_rate_p
                    ):
                        # Even if the contract accepts the previous 15 minute slot fee rate, using it is risking the contract rejecting the transaction if for whatever reason it arrives late, it is therefore always advisable to use the current 15 minute slot fee rate even if it may be higher at times
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Insufficient funds to send the value of {} {} with a fee of {} JUA, minimum fee is {} JUA".format(
                                float(contract_args["contract"]["transaction_value"]),
                                contract_args["contract"]["asset_peg"],
                                fee_received,
                                (
                                    float(
                                        contract_args["contract"]["transaction_value"]
                                    )
                                    * stakers_fee_rate_c
                                ),
                            ),
                            "requisites": requisites,
                        }

                # Check if the fee is split accordingly as per share of reserves' owner usage when transacting the contract
                received_fee_ratio_split = {}
                for rfs in iter(received_fee_share_split.items()):
                    received_fee_ratio_split[rfs[0]] = (rfs[1] / fee_received)

                expected_fee_ratio_split = {}
                for rfs in iter(received_fee_share_split.items()):
                    expected_fee_ratio_split[rfs[0]] = (rfs[1] / fee_received)

                for rfs_exp in iter(expected_fee_ratio_split.items()):
                    for rfs_rec in iter(received_fee_ratio_split.items()):
                        # We skip checking the last or only item as there is a check for adequacy already below.
                        if (rfs_exp[0] != last_item[1]) and (rfs_rec[0] == rfs_exp[0]) and (rfs_rec[1] < rfs_exp[1]):
                            storage.db.session.rollback()
                            return {
                                "status": "error",
                                "contract_response_code": 0,
                                "contract_response_hash": str(),
                                "content": "Unexpected fee share split",
                                "requisites": requisites,
                            }
                
                value_remainder_items = None
                amount_lock_items = None
                value_items = None
                quote_price_item = None
                for fl in iter(issuer_lock_ratio_amounts.items()):
                    for ty in (fl[1].values()):
                        if fl[0] == 'value_remainder_fill':
                            value_remainder_items = fl[1]

                        if fl[0] == 'amount_lock_remainder_fill':
                            amount_lock_items = fl[1]

                        if fl[0] == 'value_fill':
                            value_items = fl[1]

                        if fl[0] == 'quote_price':
                            quote_price_item = fl[1]

                for issuer_ratio_item in iter(issuer_lock_ratio_amounts['value_fill'].items()):  # We use value_fill key to limit the number of loops to to the correct number of transactions to add to the db, we could have used any other key as they are all consistently equal in structure.
                    # Generate a contract for the receiving user
                    db_insert = storage.Contracts(
                        last_item[0],
                        contract_args["contract"]["owner_address"],
                        hashlib_jua.sha256(str(last_item[2]).encode("utf-8")).hexdigest(),
                        last_item[3],
                        issuer_ratio_item[0],
                        last_item[5],
                        round(list(value_remainder_items[issuer_ratio_item[0]].values())[0], 2),
                        10000,  # 10000 days as it's now a perpetual contract
                        list(issuer_ratio_item[1].keys())[0],
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0], 4),
                        round(list(value_items[issuer_ratio_item[0]].values())[0], 2),
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        packet_id,
                        list(quote_price_item[issuer_ratio_item[0]].values())[0],
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (last_item[0],
                        contract_args["contract"]["owner_address"],
                        hashlib_jua.sha256(str(last_item[2]).encode("utf-8")).hexdigest(),
                        last_item[3],
                        issuer_ratio_item[0],
                        last_item[5],
                        round(list(value_remainder_items[issuer_ratio_item[0]].values())[0], 2),
                        10000,  # 10000 days as it's now a perpetual contract
                        list(issuer_ratio_item[1].keys())[0],
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0], 4),
                        round(list(value_items[issuer_ratio_item[0]].values())[0], 2),
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        packet_id,
                        list(quote_price_item[issuer_ratio_item[0]].values())[0],)

                # raise Exception('test')
                # Generate the contract change if there is any
                if value_remainder_fill > 0:
                    db_insert__ = storage.Contracts(
                        last_item[0],
                        last_item[1],
                        hashlib_jua.sha256(
                            str(last_item[2]).encode("utf-8")
                        ).hexdigest(),
                        last_item[3],
                        last_item[4],
                        last_item[5],
                        round(
                            value_remainder_fill,
                            2,
                        ),
                        10000,  # 10000 days as it's now a perpetual contract
                        last_item[8],
                        round(amount_lock_remainder_fill,
                            4,
                        ),
                        0,
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        packet_id,
                        last_item[17],
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert__)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (last_item[0],
                        last_item[1],
                        hashlib_jua.sha256(
                            str(last_item[2]).encode("utf-8")
                        ).hexdigest(),
                        last_item[3],
                        last_item[4],
                        last_item[5],
                        round(
                            value_remainder_fill,
                            2,
                        ),
                        10000,  # 10000 days as it's now a perpetual contract
                        last_item[8],
                        round(amount_lock_remainder_fill,
                            4,
                        ),
                        0,
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        packet_id,
                        last_item[17],)

                # Disable the used contracts
                obj_count___________ = 0
                loop_obj___________ = disabled_sequential_hashes_list[:]
                for ds in disabled_sequential_hashes_list:
                    loop_obj___________[obj_count___________] = (
                        storage.db.session.query(storage.Contracts)
                        .filter(
                            storage.Contracts.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.Contracts.status == 1,
                            storage.Contracts.packet_id == ds,
                            storage.Contracts.owner_address
                            == list(packet.values())[0][
                                "sender_address"
                            ],  # This is included as more than one contract can share the same packet_id after splitting for change
                        )
                        .first()
                    )
                    if loop_obj___________[obj_count___________] != None:
                        loop_obj___________[obj_count___________].status = -1
                        obj_count___________ += 1
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "No contract available for your address to use for this transaction. Please generate a contract first or have a third party generate one for you.",
                            "requisites": requisites,
                        }

                update_based_changes__ = db.session.query(storage.Contracts.status).filter(
                            storage.Contracts.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.Contracts.status == 1,
                            storage.Contracts.owner_address
                            == list(packet.values())[0][
                                "sender_address"
                            ],  # This is included as more than one contract can share the same packet_id after splitting for change
                        ).order_by(storage.Contracts.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                self.response_hash += (update_based_changes__,)

                # save the response hash
                contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                return {
                    "status": "complete",
                    "contract_response_code": 1,
                    "contract_response_hash": contract_response_hash,
                    "content": "Transaction complete",
                    "requisites": requisites,
                }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "contract_response_hash": str(),
                    "content": "Reserves contract not found",
                    "requisites": requisites,
                }

        # The issuer who has a higher ratio would simply gets selected more often by the dapps than someone with a lower ratio. Thats the only advantage. They do not get paid a higher fee than those on a lower ratio and thus make the same fees as everyone else. But in the long run make more in fees as they are selected more often than those on a lower ratio. This mechanism has a general advantage in securing the value for transactors while rewarding those that do so the most.
        elif contract_args["action"] == "lock_reserves":
            # First check if we haven't saved this packet
            if packet != {}:
                check_exist = (
                    storage.db.session.query(storage.ReserveTransactions.coin)
                    .filter(
                        storage.ReserveTransactions.asset_peg
                        == contract_args["contract"]["asset_peg"],
                        storage.ReserveTransactions.status == 1,
                        storage.ReserveTransactions.amount > 0,
                        storage.ReserveTransactions.packet_id
                        == packet_id,
                    )
                    .first()
                )

                if check_exist != None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "_1Coin already exists. Make sure the key 'contract_args' is sending a different sequential hash.",
                        "requisites": {},
                    }

                elif contract_args["contract"]["asset_peg"] not in [
                    "KES",
                    "ZAR",
                    "ZMW",
                    "NAD",
                    "USD",
                ]:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Invalid asset peg supplied",
                        "requisites": {},
                    }
                else:
                    last_item = None
                    for t in iter(packet.values()):
                        if t["transaction_type"] == "5":
                            last_item = t
                            break

                    if last_item == None:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Missing transaction in packet.",
                            "requisites": {},
                        }

                    db_insert = storage.ReserveTransactions(
                        last_item["transaction_type"],
                        str(last_item),
                        int(last_item["send_timestamp"]),
                        last_item["sender_address"],
                        1,
                        packet_id,
                        contract_args["contract"]["asset_peg"],
                        round(float(last_item["transaction_type_balance"]), 4),
                        round(float(last_item["transaction_type_balance"]), 4),
                        contract_args["contract"]["lock_ratio"],
                        "sender_generated",
                        quoted_price,
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (
                        last_item["transaction_type"],
                        str(last_item),
                        int(last_item["send_timestamp"]),
                        last_item["sender_address"],
                        1,
                        packet_id,
                        contract_args["contract"]["asset_peg"],
                        round(float(last_item["transaction_type_balance"]), 4),
                        round(float(last_item["transaction_type_balance"]), 4),
                        contract_args["contract"]["lock_ratio"],
                        "sender_generated",
                        quoted_price,)

                    # Add to the accumulation tables for storage.TotalValueLocked
                    update__ = (
                        storage.db.session.query(storage.TotalValueLocked)
                        .filter(
                            storage.TotalValueLocked.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.TotalValueLocked.lock_ratio
                            == contract_args["contract"]["lock_ratio"],
                        )
                        .first()
                    )

                    if update__ != None:
                        total_amount__ = (
                            storage.db.session.query(
                                storage.TotalValueLocked.total_contracted_amount,
                                storage.TotalValueLocked.total_reserves_amount,
                                storage.TotalValueLocked.min_operational_value,
                            )
                            .filter(
                                storage.TotalValueLocked.asset_peg
                                == contract_args["contract"]["asset_peg"],
                                storage.TotalValueLocked.lock_ratio
                                == contract_args["contract"]["lock_ratio"],
                            )
                            .first()
                        )
                        update__.total_reserves_amount = round(
                            float(total_amount__[1])
                            + round(float(last_item["transaction_type_balance"]), 4),
                            4,
                        )
                        update__.min_operational_value = round(
                            float(total_amount__[2])
                            + (
                                round(
                                    float(last_item["transaction_type_balance"])
                                    * quoted_price
                                    * float(contract_args["contract"]["lock_ratio"]),
                                    4,
                                )
                            ),
                            2,
                        )
                        update__.updated = datetime_jua.datetime.utcnow().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )

                        update_based_changes__ = db.session.query(storage.TotalValueLocked.total_reserves_amount, storage.TotalValueLocked.min_operational_value).order_by(storage.TotalValueLocked.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                        self.response_hash += (update_based_changes__,)

                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Could not access TotalValueLocked.",
                            "requisites": {},
                        }

                    # Increment Config values
                    config = (
                        storage.db.session.query(
                            storage.Config.withdrawal_datetime_id,
                            storage.Config.total_contracted_amount,
                            storage.Config.total_min_operational_value,
                            storage.Config.total_reserves_amount,
                            storage.Config.fee_rate,
                        )
                        .filter(
                            storage.Config.asset_peg
                            == contract_args["contract"]["asset_peg"]
                        )
                        .first()
                    )

                    if config != None:
                        update_ = (
                            storage.db.session.query(storage.Config)
                            .filter(
                                storage.Config.asset_peg
                                == contract_args["contract"]["asset_peg"]
                            )
                            .first()
                        )
                        update_.total_min_operational_value = round(
                            float(config[2])
                            + (
                                round(
                                    float(last_item["transaction_type_balance"])
                                    * quoted_price
                                    * float(contract_args["contract"]["lock_ratio"]),
                                    4,
                                )
                            ),
                            2,
                        )
                        update_.total_reserves_amount = round(
                            float(config[3])
                            + (
                                round(
                                    float(last_item["transaction_type_balance"])
                                    / quoted_price,
                                    4,
                                )
                            ),
                            2,
                        )
                        update_.updated = datetime_jua.datetime.utcnow().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )

                        update_based_changes__ = db.session.query(storage.Config.total_min_operational_value, storage.Config.total_reserves_amount).order_by(storage.Config.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                        self.response_hash += (update_based_changes__,)

                        # save the response hash
                        contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                        return {
                            "status": "complete",
                            "contract_response_code": 1,
                            "contract_response_hash": contract_response_hash,
                            "content": "Locked reserves for: {}".format(contract_args),
                            "requisites": {},
                        }
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Could not access Config.",
                            "requisites": {},
                        }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "contract_response_hash": str(),
                    "content": "No transaction packet supplied to complete process.",
                    "requisites": {},
                }

        # Contracts are generated on behalf of the holder of the contract by the owner of the reserves. In a future release we will allow anyone to generate using available reserves automatically as long as they pay a maintenance fee.
        elif contract_args["action"] == "generate_contract":
            # First check if we haven't saved this packet
            if packet != {}:
                check_exist = (
                    storage.db.session.query(storage.ReserveTransactions.coin)
                    .filter(
                        storage.ReserveTransactions.issuer_address
                        == list(packet.values())[0]["sender_address"],
                        storage.ReserveTransactions.asset_peg
                        == contract_args["contract"]["asset_peg"],
                        storage.ReserveTransactions.status == 1,
                        storage.ReserveTransactions.amount > 0,
                        storage.ReserveTransactions.packet_id
                        == packet_id,
                    )
                    .first()
                )

                if check_exist != None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "_2Coin already exists",
                        "requisites": {},
                    }

                # Needed for the asset name below
                elif contract_args["contract"]["asset_peg"] not in [
                    "KES",
                    "ZAR",
                    "ZMW",
                    "NAD",
                    "USD",
                ]:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Invalid asset peg supplied",
                        "requisites": {},
                    }

                elif (contract_args["contract"]["owner_address"][-4:] == '.jua') or ('.jua.' in contract_args["contract"]["owner_address"]):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "The juapay contract does not support UUID via the API interface.",
                        "requisites": {},
                    }
                
                else:
                    # Check that the issuer_address has enough reserves to create this contract at the given ratio level. The issuer cannot mix different ratio levels to fulfill a single order and therefore must have enough to cover the single order at the given ratio level, that feature will be available in future when contracts are created without the issuer granting them out.
                    owner_reserves = (
                        storage.db.session.query(
                            storage.ReserveTransactions.id,  # Deleted field but risky to remove loop references in code so left to prevent a typo error being made, field will be used in future versions
                            storage.ReserveTransactions.transaction_type,
                            storage.ReserveTransactions.coin,
                            storage.ReserveTransactions.sent_date_id,
                            storage.ReserveTransactions.issuer_address,
                            storage.ReserveTransactions.status,
                            storage.ReserveTransactions.id,  # Deleted field but risky to remove loop references in code so left to prevent a typo error being made, field will be used in future versions
                            storage.ReserveTransactions.id,  # Deleted field but risky to remove loop references in code so left to prevent a typo error being made, field will be used in future versions
                            storage.ReserveTransactions.packet_id,
                            storage.ReserveTransactions.amount,
                            storage.ReserveTransactions.lock_ratio,
                            storage.ReserveTransactions.type,
                            storage.ReserveTransactions.asset_peg,
                            storage.ReserveTransactions.transaction_value,
                        )
                        .filter(
                            storage.ReserveTransactions.issuer_address
                            == list(packet.values())[0]["sender_address"],
                            storage.ReserveTransactions.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.ReserveTransactions.status == 1,
                            storage.ReserveTransactions.amount > 0,
                            storage.ReserveTransactions.packet_id.in_(
                                contract_args["contract"][
                                    "claimed_packet_ids"
                                ]
                            ),
                        )
                        .all()
                    )

                    if owner_reserves == []:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "No reserves to create contract found for reserves sequential hashes: {}".format(
                                contract_args["contract"][
                                    "claimed_packet_ids"
                                ]
                            ),
                            "requisites": {},
                        }

                    else:
                        # Check the sender has enough reserves at current market value to generate the contract
                        total_issuer_reserves = 0
                        last_item___ = None
                        disabled_sequential_hashes_list = []
                        for rsv in owner_reserves:
                            total_issuer_reserves += float(rsv[9])
                            last_item___ = rsv
                            disabled_sequential_hashes_list.append(rsv[8])
                            if total_issuer_reserves >= round(
                                (
                                    float(contract_args["contract"]["value"])
                                    / quoted_price
                                )
                                * float(rsv[10]),
                                4,
                            ):
                                break

                        if total_issuer_reserves < round(
                            (float(contract_args["contract"]["value"]) / quoted_price)
                            * float(last_item___[10]),
                            4,
                        ):
                            storage.db.session.rollback()
                            return {
                                "status": "error",
                                "contract_response_code": 0,
                                "contract_response_hash": str(),
                                "content": "Insufficient reserves of {} to generate contract of {} {} at {}x with quoted price {}".format(
                                    total_issuer_reserves,
                                    round(
                                        float(contract_args["contract"]["value"]),
                                        4,
                                    ),
                                    contract_args["contract"]["asset"],
                                    last_item___[10],
                                    quoted_price,
                                ),
                                "requisites": {},
                            }
                        else:
                            if contract_args != {}:
                                # Generate a reserves smart_contract to represent the value of Juana locked for the user
                                db_insert = storage.Contracts(
                                    str(
                                        contract_args["contract"][
                                            "claimed_packet_ids"
                                        ]
                                    ),
                                    contract_args["contract"]["owner_address"],
                                    hashlib_jua.sha256(
                                        str(contract_args["contract"]).encode("utf-8")
                                    ).hexdigest(),
                                    (
                                        "Kenyan Shillings"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "KES"
                                        )
                                        else "Zambian Kwacha"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "ZMW"
                                        )
                                        else "South African Rand"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "ZAR"
                                        )
                                        else "Namibian Dollar"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "NAD"
                                        )
                                        else "United States Dollar"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "USD"
                                        )
                                        else "N/A"
                                    ),
                                    owner_reserves[0][4],
                                    contract_args["contract"]["asset_peg"],
                                    float(contract_args["contract"]["value"]),
                                    30,
                                    last_item___[10],
                                    round(
                                        (
                                            float(contract_args["contract"]["value"])
                                            / quoted_price
                                        ),
                                        4,
                                    ),
                                    0,
                                    0,
                                    contract_args["contract"]["utc_time"],
                                    contract_args["contract"]["receipt_date_id"],
                                    "non-perpetual",
                                    str(contract_args),
                                    1,
                                    packet_id,
                                    quoted_price,
                                    datetime_jua.datetime.utcnow().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                )

                                for x in range(3):
                                    try:
                                        storage.db.session.add(db_insert)
                                        break
                                    except:
                                        storage.db.session.rollback()
                                        if x == 2:
                                            raise Exception(
                                                "Could not access the database upon insert."
                                            )

                                self.response_hash += (str(
                                        contract_args["contract"][
                                            "claimed_packet_ids"
                                        ]
                                    ),
                                    contract_args["contract"]["owner_address"],
                                    hashlib_jua.sha256(
                                        str(contract_args["contract"]).encode("utf-8")
                                    ).hexdigest(),
                                    (
                                        "Kenyan Shillings"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "KES"
                                        )
                                        else "Zambian Kwacha"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "ZMW"
                                        )
                                        else "South African Rand"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "ZAR"
                                        )
                                        else "Namibian Dollar"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "NAD"
                                        )
                                        else "United States Dollar"
                                        if (
                                            contract_args["contract"]["asset_peg"]
                                            == "USD"
                                        )
                                        else "N/A"
                                    ),
                                    owner_reserves[0][4],
                                    contract_args["contract"]["asset_peg"],
                                    float(contract_args["contract"]["value"]),
                                    30,
                                    last_item___[10],
                                    round(
                                        (
                                            float(contract_args["contract"]["value"])
                                            / quoted_price
                                        ),
                                        4,
                                    ),
                                    0,
                                    0,
                                    contract_args["contract"]["utc_time"],
                                    contract_args["contract"]["receipt_date_id"],
                                    "non-perpetual",
                                    str(contract_args),
                                    1,
                                    packet_id,
                                    1,)

                                # Generate reserves change if any and disable the used reserves contracts
                                remaining_reserves_fill = (
                                    total_issuer_reserves  # This becomes the change
                                )
                                last_item = None

                                remaining_fill = round(
                                    (float(contract_args["contract"]["value"]) / quoted_price)
                                    * float(last_item___[10]),
                                    4,
                                )

                                # Notice the obj_count is not placed here as it may get reused and cause an error as its inside another loop
                                for req in owner_reserves:
                                    if remaining_fill <= 0:
                                        last_item = req
                                        break

                                    elif remaining_reserves_fill >= remaining_fill:
                                        # Then we know we are on the last loop
                                        last_item = req
                                        break
                                    else:
                                        remaining_fill -= float(req[9])

                                    if (req == owner_reserves[-1]) and (
                                        remaining_fill > 0
                                    ):
                                        storage.db.session.rollback()
                                        return {
                                            "status": "error",
                                            "contract_response_code": 0,
                                            "contract_response_hash": str(),
                                            "content": "Insufficient reserves of {} to generate contract of {} {} at {}x with quoted price {}".format(
                                                total_issuer_reserves,
                                                round(
                                                    (
                                                        float(
                                                            contract_args["contract"][
                                                                "value"
                                                            ]
                                                        )
                                                        / quoted_price
                                                    ),
                                                    4,
                                                ),
                                                contract_args["contract"]["asset"],
                                                last_item___[10],
                                                quoted_price,
                                            ),
                                            "requisites": {},
                                        }

                                # Deduct the original value of remaining_fill from remaining_reserves_fill
                                remaining_reserves_fill -= round(
                                    (float(contract_args["contract"]["value"]) / quoted_price)
                                    * float(last_item___[10]),
                                    4,
                                )

                                # Put the change back into the reserves
                                if remaining_reserves_fill > 0:
                                    db_insert = storage.ReserveTransactions(
                                        last_item[1],
                                        last_item[2],
                                        int(last_item[3]),
                                        last_item[4],
                                        1,
                                        packet_id,
                                        last_item[12],
                                        round(float(remaining_reserves_fill), 4),
                                        round(float(contract_args["contract"]["value"]), 4),
                                        last_item[10],
                                        "change_generated",
                                        quoted_price,
                                        datetime_jua.datetime.utcnow().strftime(
                                            "%Y-%m-%d %H:%M:%S"
                                        ),
                                    )

                                    for x in range(3):
                                        try:
                                            storage.db.session.add(db_insert)
                                            break
                                        except:
                                            storage.db.session.rollback()
                                            if x == 2:
                                                raise Exception(
                                                    "Could not access the database upon insert."
                                                )

                                    self.response_hash += (
                                        last_item[1],
                                        last_item[2],
                                        int(last_item[3]),
                                        last_item[4],
                                        1,
                                        packet_id,
                                        last_item[12],
                                        round(float(remaining_reserves_fill), 4),
                                        round(float(contract_args["contract"]["value"]), 4),
                                        last_item[10],
                                        "change_generated",
                                        quoted_price,)

                                # Disable the used reserves
                                obj_count_____________ = 0
                                loop_obj_____________ = disabled_sequential_hashes_list[
                                    :
                                ]
                                for ds in disabled_sequential_hashes_list:
                                    loop_obj_____________[obj_count_____________] = (
                                        storage.db.session.query(
                                            storage.ReserveTransactions
                                        )
                                        .filter(
                                            storage.ReserveTransactions.asset_peg
                                            == contract_args["contract"]["asset_peg"],
                                            storage.ReserveTransactions.status == 1,
                                            storage.ReserveTransactions.amount > 0,
                                            storage.ReserveTransactions.packet_id
                                            == ds,
                                        )
                                        .first()
                                    )
                                    loop_obj_____________[
                                        obj_count_____________
                                    ].status = (
                                        -1
                                    )  # We permanently disable because when the generated contract is sold back into the reserves it will create a new record for reserves
                                    obj_count_____________ += 1

                                update_based_changes__ = db.session.query(storage.ReserveTransactions.status).filter(
                                            storage.ReserveTransactions.asset_peg
                                            == contract_args["contract"]["asset_peg"],
                                            storage.ReserveTransactions.status == 1,
                                            storage.ReserveTransactions.amount > 0,
                                        ).order_by(storage.ReserveTransactions.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                                self.response_hash += (update_based_changes__,)

                                # Add to the accumulation tables for storage.TotalValueLocked
                                total_amount_ = (
                                    storage.db.session.query(
                                        storage.TotalValueLocked.total_contracted_amount,
                                        storage.TotalValueLocked.total_reserves_amount,
                                        storage.TotalValueLocked.min_operational_value,
                                        storage.TotalValueLocked.lock_ratio,
                                    )
                                    .filter(
                                        storage.TotalValueLocked.lock_ratio
                                        == last_item___[10],
                                        storage.TotalValueLocked.asset_peg
                                        == contract_args["contract"]["asset_peg"],
                                    )
                                    .first()
                                )
                                update____ = (
                                    storage.db.session.query(storage.TotalValueLocked)
                                    .filter(
                                        storage.TotalValueLocked.lock_ratio
                                        == last_item___[10],
                                        storage.TotalValueLocked.asset_peg
                                        == contract_args["contract"]["asset_peg"],
                                    )
                                    .first()
                                )
                                if update____ != None:
                                    update____.total_contracted_amount = round(
                                        float(total_amount_[0])
                                        + float(
                                            round(
                                                (
                                                    float(
                                                        contract_args["contract"][
                                                            "value"
                                                        ]
                                                    )
                                                    / quoted_price
                                                ),
                                                4,
                                            )
                                        ),
                                        4,
                                    )
                                    update____.total_reserves_amount = round(
                                        float(total_amount_[1])
                                        - float(
                                            round(
                                                (
                                                    float(
                                                        contract_args["contract"][
                                                            "value"
                                                        ]
                                                    )
                                                    / quoted_price
                                                ),
                                                4,
                                            )
                                        ),
                                        4,
                                    )

                                    update____.updated = (
                                        datetime_jua.datetime.utcnow().strftime(
                                            "%Y-%m-%d %H:%M:%S"
                                        )
                                    )

                                    update_based_changes__ = db.session.query(storage.TotalValueLocked.total_contracted_amount, storage.TotalValueLocked.total_reserves_amount).order_by(storage.TotalValueLocked.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                                    self.response_hash += (update_based_changes__,)

                                else:
                                    storage.db.session.rollback()
                                    return {
                                        "status": "error",
                                        "contract_response_code": 0,
                                        "contract_response_hash": str(),
                                        "content": "Could not access TotalValueLocked.",
                                        "requisites": {},
                                    }

                                # Increment Config values
                                config = (
                                    storage.db.session.query(
                                        storage.Config.withdrawal_datetime_id,
                                        storage.Config.total_contracted_amount,
                                        storage.Config.total_min_operational_value,
                                        storage.Config.total_reserves_amount,
                                        storage.Config.fee_rate,
                                    )
                                    .filter(
                                        storage.Config.asset_peg
                                        == contract_args["contract"]["asset_peg"]
                                    )
                                    .first()
                                )

                                if config != None:
                                    update_ = (
                                        storage.db.session.query(storage.Config)
                                        .filter(
                                            storage.Config.asset_peg
                                            == contract_args["contract"]["asset_peg"]
                                        )
                                        .first()
                                    )
                                    update_.total_contracted_amount = round(
                                        float(config[1])
                                        + float(
                                            round(
                                                (
                                                    float(
                                                        contract_args["contract"][
                                                            "value"
                                                        ]
                                                    )
                                                    / quoted_price
                                                ),
                                                4,
                                            )
                                        ),
                                        4,
                                    )
                                    update_.total_reserves_amount = round(
                                        float(config[3])
                                        - float(
                                            round(
                                                (
                                                    float(
                                                        contract_args["contract"][
                                                            "value"
                                                        ]
                                                    )
                                                    / quoted_price
                                                ),
                                                4,
                                            )
                                        ),
                                        4,
                                    )
                                    update_.updated = (
                                        datetime_jua.datetime.utcnow().strftime(
                                            "%Y-%m-%d %H:%M:%S"
                                        )
                                    )

                                    update_based_changes__ = db.session.query(storage.Config.total_contracted_amount, storage.Config.total_reserves_amount).order_by(storage.Config.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                                    self.response_hash += (update_based_changes__,)

                                    # save the response hash
                                    contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                                    return {
                                        "status": "complete",
                                        "contract_response_code": 1,
                                        "contract_response_hash": contract_response_hash,
                                        "content": "Generated reserves contract: {}".format(
                                            contract_args
                                        ),
                                        "requisites": {},
                                    }
                                else:
                                    storage.db.session.rollback()
                                    return {
                                        "status": "error",
                                        "contract_response_code": 0,
                                        "contract_response_hash": str(),
                                        "content": "Could not access Config.",
                                        "requisites": {},
                                    }
                            else:
                                storage.db.session.rollback()
                                return {
                                    "status": "error",
                                    "contract_response_code": 0,
                                    "contract_response_hash": str(),
                                    "content": "Received an empty contract_args argument",
                                    "requisites": {},
                                }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "contract_response_hash": str(),
                    "content": "No transaction packet supplied to complete process.",
                    "requisites": {},
                }

        # Place withdraw reserves order
        # Orders are executed based on the available withdrawable amount which is the excess of reserves
        elif contract_args["action"] == "withdrawal_order":
            if contract_args != {}:
                if float(contract_args["withdraw_reserves_amount"]) <= 0:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Invalid reserves amount to withdraw",
                        "requisites": {"contract_lock_status": 1},
                    }

                # Verify they have a valid reserve
                owner_reserves = (
                    storage.db.session.query(
                        storage.ReserveTransactions.id,  # Deleted field but risky to remove loop references in code so left to prevent a typo error being made, field will be used in future versions
                        storage.ReserveTransactions.transaction_type,
                        storage.ReserveTransactions.coin,
                        storage.ReserveTransactions.sent_date_id,
                        storage.ReserveTransactions.issuer_address,
                        storage.ReserveTransactions.status,
                        storage.ReserveTransactions.id,  # Deleted field but risky to remove loop references in code so left to prevent a typo error being made, field will be used in future versions
                        storage.ReserveTransactions.id,  # Deleted field but risky to remove loop references in code so left to prevent a typo error being made, field will be used in future versions
                        storage.ReserveTransactions.packet_id,
                        storage.ReserveTransactions.amount,
                        storage.ReserveTransactions.lock_ratio,
                        storage.ReserveTransactions.type,
                        storage.ReserveTransactions.quote_price,
                        storage.ReserveTransactions.asset_peg,
                        storage.ReserveTransactions.transaction_value,
                    )
                    .filter(
                        storage.ReserveTransactions.status == 1,
                        storage.ReserveTransactions.amount > 0,
                        storage.ReserveTransactions.issuer_address
                        == list(packet.values())[0]["sender_address"],
                    )
                    .order_by(storage.ReserveTransactions.lock_ratio.asc()).all()  # We allow withdrawing the smaller lock ratio first
                )

                if owner_reserves == []:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Reserves not found for {}".format(
                            list(packet.values())[0]["sender_address"]
                        ),
                        "requisites": {"contract_lock_status": 1},
                    }

                total_value_locked__ = (
                    storage.db.session.query(storage.Config.total_min_operational_value)
                    .filter(storage.Config.asset_peg == contract_args["asset_peg"])
                    .first()
                )

                # Check if there is enough for the requested amount
                ratio_level_amounts = {}  # Format {ratio_level: amount}
                available_funds = 0
                min_operational_value = 0
                total_available_reserves = round(
                    float(total_value_locked__[0])
                    - (float(total_value_locked__[0]) * 0.3),
                    4,
                )  # Notice we allow a 30% extra leeway of reserves to keep the 100% collaterisation from falling below if market prices change
                remaining_fill = round(
                    float(contract_args["withdraw_reserves_amount"]), 4
                )
                for req in owner_reserves:
                    if total_available_reserves >= float(
                        contract_args["withdraw_reserves_amount"]
                    ):
                        available_funds += float(req[9])
                        if available_funds >= float(
                            contract_args["withdraw_reserves_amount"]
                        ):
                            if (float(req[9]) - remaining_fill) < 0:
                                storage.db.session.rollback()
                                return {
                                    "status": "error",
                                    "contract_response_code": 0,
                                    "contract_response_hash": str(),
                                    "content": "Insufficient funds to perform action. You can only withdraw to the value of {} subject to there being enough liquidity above 100% collateralisation.".format(
                                        total_available_reserves
                                    ),
                                    "requisites": {"contract_lock_status": 1},
                                }
                            else:
                                # The last item will either be 0 or have a remainder
                                remaining_fill = round(
                                    float(req[9]) - remaining_fill, 4
                                )
                                total_available_reserves -= float(req[9])

                                min_operational_value += (
                                    (float(req[9]) - (float(req[9]) - remaining_fill))
                                    * float(req[10])
                                    * float(req[12])
                                )

                                if req[10] not in iter(ratio_level_amounts.items()):
                                    ratio_level_amounts[req[10]] = [
                                        0,
                                        0,
                                    ]  # [quantity, quoted_price]

                                ratio_level_amounts[req[10]][0] += float(req[9]) - (
                                    float(req[9]) - remaining_fill
                                )
                                break
                        else:
                            remaining_fill -= float(req[9])
                            total_available_reserves -= float(req[9])
                            min_operational_value += (
                                float(req[9]) * float(req[10]) * float(req[12])
                            )
                            if req[10] not in iter(ratio_level_amounts.items()):
                                ratio_level_amounts[req[10]] = [
                                    0,
                                    0,
                                ]  # [quantity, quoted_price]

                            ratio_level_amounts[req[10]][0] += float(req[9])

                # You cannot place orders more than 3 days in advance or less than today's date
                if (
                    int(contract_args["withdraw_process_date_id"])
                    < int(list(packet.values())[0]["send_timestamp"][:8])
                ) or (
                    int(contract_args["withdraw_process_date_id"])
                    > int(
                        (
                            datetime_jua.datetime.utcnow()
                            + datetime_jua.timedelta(days=3)
                        ).strftime("%Y%m%d")
                    )
                ):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Your withdrawal order date cannot be less than today or more than {}".format(
                            (
                                datetime_jua.datetime.utcnow()
                                + datetime_jua.timedelta(days=3)
                            ).strftime("%Y%m%d")
                        ),
                        "requisites": {"contract_lock_status": 1},
                    }

                # Add the order to the order book
                db_insert = storage.WithdrawalOrders(
                    list(packet.values())[0]["sender_address"],
                    list(packet.values())[0]["send_timestamp"],
                    int(list(packet.values())[0]["send_timestamp"][:8]),
                    contract_args["withdraw_process_date_id"],
                    int(datetime_jua.datetime.utcnow().strftime("%Y%m%d%H%M%S")),
                    str(packet),
                    recieved_datetime_id,
                    round(float(contract_args["withdraw_reserves_amount"]), 4),
                    0,
                    1,
                    contract_args["asset_peg"],
                    packet_id,
                    datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )

                for x in range(3):
                    try:
                        storage.db.session.add(db_insert)
                        break
                    except:
                        storage.db.session.rollback()
                        if x == 2:
                            raise Exception(
                                "Could not access the database upon insert."
                            )

                self.response_hash += (list(packet.values())[0]["sender_address"],
                    list(packet.values())[0]["send_timestamp"],
                    int(list(packet.values())[0]["send_timestamp"][:8]),
                    contract_args["withdraw_process_date_id"],
                    str(packet),
                    float(contract_args["withdraw_reserves_amount"]),
                    0,
                    1,
                    contract_args["asset_peg"],
                    packet_id,)

                total_issuer_reserves = 0
                last_item___ = None
                for rsv in owner_reserves:
                    total_issuer_reserves += float(rsv[9])
                    last_item___ = rsv
                    if total_issuer_reserves >= round(
                        float(contract_args["withdraw_reserves_amount"]), 4
                    ):
                        break

                # Generate reserves change if any and disable the used reserves contracts
                remaining_reserves_fill = (
                    total_issuer_reserves  # This becomes the change
                )
                last_item = None
                disabled_sequential_hashes_list = []

                remaining_fill = round(
                    float(contract_args["withdraw_reserves_amount"]), 4
                )

                # Notice the obj_count is not placed here as it may get reused and cause an error as its inside another loop
                for req in owner_reserves:
                    if remaining_fill <= 0:
                        last_item = req
                        break

                    else:
                        remaining_fill -= float(req[9])
                        disabled_sequential_hashes_list.append(req[8])
                        last_item = req

                    if (req == owner_reserves[-1]) and (remaining_fill > 0):
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Insufficient reserves of {} to withdraw reserves of {} at {}x".format(
                                total_issuer_reserves,
                                round(
                                    float(contract_args["withdraw_reserves_amount"]),
                                    4,
                                ),
                                last_item___[10],
                            ),
                            "requisites": {},
                        }
                
                # Deduct the original value of remaining_fill from remaining_reserves_fill
                remaining_reserves_fill -= round(
                    float(contract_args["withdraw_reserves_amount"]),
                    4,
                )

                # Put the change back into the reserves
                if remaining_reserves_fill > 0:
                    db_insert = storage.ReserveTransactions(
                        last_item[1],
                        last_item[2],
                        int(last_item[3]),
                        last_item[4],
                        1,
                        packet_id,
                        last_item[13],
                        round(float(remaining_reserves_fill), 4),
                        round(float(contract_args["withdraw_reserves_amount"]), 4),
                        last_item[10],
                        "change_generated",
                        quoted_price,
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (
                        last_item[1],
                        last_item[2],
                        int(last_item[3]),
                        last_item[4],
                        1,
                        packet_id,
                        last_item[13],
                        round(float(remaining_reserves_fill), 4),
                        round(float(contract_args["withdraw_reserves_amount"]), 4),
                        last_item[10],
                        "change_generated",
                        quoted_price,)

                # Disable the used reserves
                obj_count_____________ = 0
                loop_obj_____________ = disabled_sequential_hashes_list[:]
                for ds in disabled_sequential_hashes_list:
                    loop_obj_____________[obj_count_____________] = (
                        storage.db.session.query(storage.ReserveTransactions)
                        .filter(
                            storage.ReserveTransactions.asset_peg
                            == contract_args["asset_peg"],
                            storage.ReserveTransactions.status == 1,
                            storage.ReserveTransactions.amount > 0,
                            storage.ReserveTransactions.packet_id == ds,
                        )
                        .first()
                    )
                    loop_obj_____________[
                        obj_count_____________
                    ].status = (
                        -1
                    )  # We permanently disable because when the generated contract is sold back into the reserves it will create a new record for reserves
                    obj_count_____________ += 1

                update_based_changes__ = db.session.query(storage.ReserveTransactions.status).order_by(storage.ReserveTransactions.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                self.response_hash += (update_based_changes__,)

                # Add to the accumulation tables for storage.TotalValueLocked
                obj_count__________ = 0
                loop_obj__________ = list(ratio_level_amounts.items())
                for rl in iter(ratio_level_amounts.items()):
                    total_amount_ = (
                        storage.db.session.query(
                            storage.TotalValueLocked.total_contracted_amount,
                            storage.TotalValueLocked.total_reserves_amount,
                            storage.TotalValueLocked.min_operational_value,
                        )
                        .filter(
                            storage.TotalValueLocked.lock_ratio == rl[0],
                            storage.TotalValueLocked.asset_peg
                            == contract_args["asset_peg"],
                        )
                        .first()
                    )
                    loop_obj__________[obj_count__________] = (
                        storage.db.session.query(storage.TotalValueLocked)
                        .filter(
                            storage.TotalValueLocked.lock_ratio == rl[0],
                            storage.TotalValueLocked.asset_peg
                            == contract_args["asset_peg"],
                        )
                        .first()
                    )
                    if loop_obj__________[obj_count__________] != None:
                        loop_obj__________[
                            obj_count__________
                        ].total_reserves_amount = round(
                            float(total_amount_[1])
                            - (rl[1][0] - (rl[1][0] - remaining_fill)),
                            4,
                        )
                        loop_obj__________[
                            obj_count__________
                        ].min_operational_value = round(
                            float(total_amount_[2]) - (round(min_operational_value, 4)),
                            2,
                        )
                        loop_obj__________[
                            obj_count__________
                        ].updated = datetime_jua.datetime.utcnow().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        obj_count__________ += 1

                        update_based_changes__ = db.session.query(storage.TotalValueLocked.total_reserves_amount, storage.TotalValueLocked.min_operational_value).order_by(storage.TotalValueLocked.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                        self.response_hash += (update_based_changes__,)

                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Could not access TotalValueLocked.",
                            "requisites": {},
                        }

                # Increment Config values
                config = (
                    storage.db.session.query(
                        storage.Config.withdrawal_datetime_id,
                        storage.Config.total_contracted_amount,
                        storage.Config.total_min_operational_value,
                        storage.Config.total_reserves_amount,
                        storage.Config.fee_rate,
                    )
                    .filter(storage.Config.asset_peg == contract_args["asset_peg"])
                    .first()
                )

                if config != None:
                    update = (
                        storage.db.session.query(storage.Config)
                        .filter(storage.Config.asset_peg == contract_args["asset_peg"])
                        .first()
                    )

                    update.total_reserves_amount = round(
                        float(config[3])
                        - (
                            sum(list(ratio_level_amounts.values())[0])
                            - (sum(list(ratio_level_amounts.values())[0]) - remaining_fill)
                        ),
                        4,
                    )
                    update.total_min_operational_value = round(
                        float(config[2]) - (round(min_operational_value, 4)), 2
                    )
                    update.updated = datetime_jua.datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                    update_based_changes__ = db.session.query(storage.Config.total_reserves_amount, storage.Config.total_min_operational_value).order_by(storage.Config.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                    self.response_hash += (update_based_changes__,)

                else:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Could not access Config.",
                        "requisites": {"contract_lock_status": 1},
                    }

                # Check if there is enough reserve to withdraw above the 100% collateralisation level
                total_value_locked__ = (
                    storage.db.session.query(
                        storage.Config.total_reserves_amount,
                        storage.Config.total_min_operational_value,
                    )
                    .filter(storage.Config.asset_peg == contract_args["asset_peg"])
                    .first()
                )

                if float(total_value_locked__[1]) < (
                    float(total_value_locked__[0]) * quoted_price
                ):
                    # save the response hash
                    contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                    return {
                        "status": "complete",
                        "contract_response_code": 1,
                        "contract_response_hash": contract_response_hash,
                        "content": "Reserves below operational levels to allow withdrawals, this order has been queued to be processed later today at close of day 23:59:59 UTC should there be an excess in reserves above 100% collateralisation.",
                        "requisites": {
                            "contract_lock_status": 1
                        },  # We do not change the lock status as it will be changed at close of day
                    }

                else:
                    # Disable the order and send it to the user's wallet
                    update = (
                        storage.db.session.query(storage.WithdrawalOrders)
                        .filter(
                            storage.WithdrawalOrders.packet_id == packet_id
                        )
                        .first()
                    )
                    if update != None:
                        update.status = -1

                        update_based_changes__ = db.session.query(storage.WithdrawalOrders.status).order_by(storage.WithdrawalOrders.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                        self.response_hash += (update_based_changes__,)

                        # save the response hash
                        contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                        return {
                            "status": "complete",
                            "contract_response_code": 1,
                            "contract_response_hash": contract_response_hash,
                            "content": "Withdrawal order sent to wallet {}, amount {}".format(
                                list(packet.values())[0]["sender_address"],
                                float(contract_args["withdraw_reserves_amount"]),
                            ),
                            "requisites": {"contract_lock_status": 0},
                        }

            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "contract_response_hash": str(),
                    "content": "Reserves contract not found",
                    "requisites": {"contract_lock_status": 1},
                }

        # Sell a contract back to reserves in exchange for Juana
        elif contract_args["action"] == "sell_contract":
            if contract_args != {}:
                issuer_lock_ratio_amounts = {}
                
                # Ask the node to save the requisites if it does not have any saved for this contract
                if ("current_15m" not in iter(requisites.keys())) or (
                    "previous_15m" not in iter(requisites.keys())
                ):
                    requisites = {
                        "save": 1,
                        "contract_lock_status": 1,
                        "current_15m": {
                            "stakers_fee_rate": 0.001,
                            "withdrawal_rate": 0.001,
                        },
                        "previous_15m": {
                            "stakers_fee_rate": 0.001,
                            "withdrawal_rate": 0.001,
                        },
                    }  # Requirements that can verify that a contract execution will be successful or not, prior to when the execution occurs e.g. a fee charge required to execute the contract may need to be known prior to the end user executing the contract so that they do not spend any blockchain fee only for their contract fee to be declined by the smart contract. These values are saved by the node every 15 minutes if the 'save' parameter is set to 1. Only the 'save' parameter is standard, the rest are only relevant to this specific contract

                    storage.db.session.rollback()
                    return {
                        "status": "complete",  # NB: when saving requisites, always use 'complete' as status so the node commits the changes else they will not
                        "contract_response_code": 1,
                        "contract_response_hash": str(),
                        "content": "Missing required parameter for requisites 'save', see documentation. Request to save parameter sent. Rerun last transaction.",
                        "requisites": requisites,
                    }

                # Get both the current and previous 15 minute's rates, if the current rate period has passed the contract will allow using the previous 15 minute's rate
                stakers_fee_rate_c = requisites["current_15m"]["stakers_fee_rate"]
                stakers_fee_rate_p = requisites["previous_15m"]["stakers_fee_rate"]

                if float(contract_args["contract"]["amount"]) <= 0:
                    self.response_hash += (requisites,)

                    # save the response hash
                    contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": contract_response_hash,
                        "content": "Invalid amount transacted",
                        "requisites": {},
                    }

                # Verify they have valid contracts and add up to see that the total being sent is enough to send the amount
                last_item = None
                contracts__ = (
                    storage.db.session.query(
                        storage.Contracts.claimed_packet_ids,
                        storage.Contracts.owner_address,
                        storage.Contracts.hash,
                        storage.Contracts.asset,
                        storage.Contracts.issuer_address,
                        storage.Contracts.asset_peg,
                        storage.Contracts.value,
                        storage.Contracts.lock_period,
                        storage.Contracts.lock_ratio,
                        storage.Contracts.amount_lock,
                        storage.Contracts.transaction_value,
                        storage.Contracts.utc_time,
                        storage.Contracts.receipt_date_id,
                        storage.Contracts.type,
                        storage.Contracts.contract,
                        storage.Contracts.status,
                        storage.Contracts.packet_id,
                        storage.Contracts.quote_price,
                    )
                    .filter(
                        storage.Contracts.owner_address
                        == list(packet.values())[0]["sender_address"],
                        storage.Contracts.status == 1,
                        storage.Contracts.asset_peg
                        == contract_args["contract"]["asset_peg"],
                        storage.Contracts.packet_id.in_(
                            contract_args["contract"]["claimed_packet_ids"]
                        ),
                    )
                    .all()
                )

                if contracts__ == []:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "No active contracts found for sender {} and asset peg {} on these sequential hashes: {}".format(
                            list(packet.values())[0]["sender_address"],
                            contract_args["contract"]["asset_peg"],
                            contract_args["contract"]["claimed_packet_ids"],
                        ),
                        "requisites": requisites,
                    }

                amount_lock_remainder_fill = 0
                disabled_sequential_hashes_list = []
                value_remainder_fill = 0
                value_fill = 0
                issuer_lock_ratio_amounts['amount_lock_remainder_fill'] = {}
                issuer_lock_ratio_amounts['value_remainder_fill'] = {}
                issuer_lock_ratio_amounts['value_fill'] = {}
                issuer_lock_ratio_amounts['quote_price'] = {}
                for req in contracts__:
                    # *** key checks ***
                    if req[4] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[4] not in iter(issuer_lock_ratio_amounts['value_remainder_fill'].keys()):
                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['value_remainder_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] = 0

                    if req[4] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] = 0

                    if req[4] not in iter(issuer_lock_ratio_amounts['value_fill'].keys()):
                        issuer_lock_ratio_amounts['value_fill'][req[4]] = {}
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['value_fill'][req[4]].keys()):
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] = 0
                        
                    if req[4] not in iter(issuer_lock_ratio_amounts['quote_price'].keys()):
                        issuer_lock_ratio_amounts['quote_price'][req[4]] = {}
                        issuer_lock_ratio_amounts['quote_price'][req[4]][req[8]] = 0

                    if req[8] not in iter(issuer_lock_ratio_amounts['quote_price'][req[4]].keys()):
                        issuer_lock_ratio_amounts['quote_price'][req[4]][req[8]] = 0

                    # *** end of key checks ***

                    disabled_sequential_hashes_list.append(req[16])
                    if (value_remainder_fill + float(req[6])) >= contract_args["contract"][
                        "amount"
                    ]:
                        value_remainder_fill = (value_remainder_fill + float(req[6])) - contract_args[
                            "contract"
                        ]["amount"]
                        amount_lock_remainder_fill = round(value_remainder_fill / float(req[17]), 4)  # the quote price is the same used at initial generation of the contract
                        value_fill += float(req[6]) - value_remainder_fill
                        last_item = req

                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] += float(req[6]) - value_remainder_fill
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] += round(issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] / float(req[17]), 4)  # the quote price is the same used at initial generation of the contract
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] += float(req[6]) - value_remainder_fill
                        issuer_lock_ratio_amounts['quote_price'][req[4]][req[8]] += req[17]

                        break

                    else:
                        value_remainder_fill += float(req[6])
                        amount_lock_remainder_fill += float(req[9])
                        value_fill += float(req[6])

                        issuer_lock_ratio_amounts['value_remainder_fill'][req[4]][req[8]] += float(req[6])
                        issuer_lock_ratio_amounts['amount_lock_remainder_fill'][req[4]][req[8]] += float(req[9])
                        issuer_lock_ratio_amounts['value_fill'][req[4]][req[8]] += float(req[6])

                if last_item == None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Unable to find required contract items",
                        "requisites": requisites,
                    }

                # It's possible that a contract is generated using more than a single reserve, therefore we may receiving sending split fees to multiple stakers so we lump them all up before checking if the total is adequate.
                expected_fee_share_split = {}
                for req in contracts__:
                    if req[16] == last_item[16]:
                        pass # The last item gets any remaining fee, we do not police how much is sent to them as we already check that an adequate fee is received. This means in cases of a single stakers reserves being used then we also do not check here as there is already an adequacy check below. This can prevent failures in processing transactions if a minute amount was sent extra due to a minor miscalculation from a dapp.
                    else:
                        if req[4] not in iter(expected_fee_share_split.keys()):
                            expected_fee_share_split[req[4]] = float(req[6])
                        else:
                            expected_fee_share_split[req[4]] += float(req[6])

                # It's possible that a contract is generated using more than a single reserve, therefore we may receiving sending split fees to multiple stakers so we lump them all up before checking if the total is adequate.
                fee_received = 0
                fee_received_service = 0
                received_fee_share_split = {}
                for f in iter(packet.values()):
                    if (f["transaction_type"] == "9"):
                        fee_received += float(f["note_balance"])

                        if f["receiver_address"] not in iter(received_fee_share_split.keys()):
                            received_fee_share_split[f["receiver_address"]] = float(f["note_balance"])
                        else:
                            received_fee_share_split[f["receiver_address"]] += float(f["note_balance"])

                    if f["receiver_address"] == self.contract_fee_address:
                        fee_received_service += float(f["note_balance"])

                # Convert service fee to JUA and check requirement, also prevents abuse of service
                if (fee_received_service == 0) or (fee_received_service < round((float(contract_args["contract"]["amount"]) * 0.002) * quoted_price, 4)):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Insufficient service fee of {} JUA received, lower than calculated {} JUA at quoted price {} {}".format(fee_received_service, round((float(contract_args["contract"]["amount"]) * 0.002) * quoted_price, 4), quoted_price, contract_args["contract"]["asset_peg"]),
                        "requisites": requisites,
                    }

                # Check if the sender sent enough contract value to split between the receiver and the sender's change at the current market price
                if value_fill < (
                    float(contract_args["contract"]["amount"])
                ):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Insufficient contracts value receipt of {} {} to transact {} {}.".format(
                            value_fill,
                            contract_args["contract"]["asset_peg"],
                            contract_args["contract"]["amount"],
                            contract_args["contract"]["asset_peg"],
                        ),
                        "requisites": {},
                    }

                value_remainder_items = None
                amount_lock_items = None
                value_items = None
                quote_price_item = None
                for fl in iter(issuer_lock_ratio_amounts.items()):
                    for ty in (fl[1].values()):
                        if fl[0] == 'value_remainder_fill':
                            value_remainder_items = fl[1]

                        if fl[0] == 'amount_lock_remainder_fill':
                            amount_lock_items = fl[1]

                        if fl[0] == 'value_fill':
                            value_items = fl[1]

                        if fl[0] == 'quote_price':
                            quote_price_item = fl[1]

                # Generate the contract change if there is any
                if value_remainder_fill > 0:
                    db_insert__ = storage.Contracts(
                        last_item[0],
                        last_item[1],
                        hashlib_jua.sha256(
                            str(last_item[2]).encode("utf-8")
                        ).hexdigest(),
                        last_item[3],
                        last_item[4],
                        last_item[5],
                        round(
                            value_remainder_fill,
                            2,
                        ),
                        10000,  # 10000 days as it's now a perpetual contract
                        last_item[8],
                        round(amount_lock_remainder_fill,
                            4,
                        ),
                        0,
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        packet_id,
                        last_item[17],
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert__)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (last_item[0],
                        last_item[1],
                        hashlib_jua.sha256(
                            str(last_item[2]).encode("utf-8")
                        ).hexdigest(),
                        last_item[3],
                        last_item[4],
                        last_item[5],
                        round(
                            value_remainder_fill,
                            2,
                        ),
                        10000,  # 10000 days as it's now a perpetual contract
                        last_item[8],
                        round(amount_lock_remainder_fill,
                            4,
                        ),
                        0,
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        packet_id,
                        last_item[17],)

                # Disable the used contracts
                obj_count___________ = 0
                loop_obj___________ = disabled_sequential_hashes_list[:]
                for ds in disabled_sequential_hashes_list:
                    loop_obj___________[obj_count___________] = (
                        storage.db.session.query(storage.Contracts)
                        .filter(
                            storage.Contracts.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.Contracts.status == 1,
                            storage.Contracts.packet_id == ds,
                            storage.Contracts.owner_address
                            == list(packet.values())[0][
                                "sender_address"
                            ],  # This is included as more than one contract can share the same packet_id after splitting for change
                        )
                        .first()
                    )
                    if loop_obj___________[obj_count___________] != None:
                        loop_obj___________[obj_count___________].status = -1
                        obj_count___________ += 1
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "No contract available for your address to use for this transaction. Please generate a contract first or have a third party generate one for you.",
                            "requisites": requisites,
                        }

                update_based_changes__ = db.session.query(storage.Contracts.status).filter(
                            storage.Contracts.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.Contracts.status == 1,
                            storage.Contracts.owner_address
                            == list(packet.values())[0][
                                "sender_address"
                            ],  # This is included as more than one contract can share the same packet_id after splitting for change
                        ).order_by(storage.Contracts.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                self.response_hash += (update_based_changes__,)

                # save the response hash
                contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()

                # Return reserves back to the same issuer at the borrowed ratio level. We insert using the newly received transaction packet details instead of update existing reserve items as multiple reserves may have been disabled to create a single contract
                last_packet_item = None
                for t in iter(packet.values()):
                    if t["transaction_type"] == "5":
                        last_packet_item = t
                        break
    
                if last_packet_item == None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Missing transaction in packet.",
                        "requisites": {},
                    }

                for issuer_ratio_item in iter(issuer_lock_ratio_amounts['value_fill'].items()):  # We use value_fill key to limit the number of loops to to the correct number of transactions to add to the db, we could have used any other key as they are all consistently equal in structure.
                    db_insert = storage.ReserveTransactions(
                        last_packet_item["transaction_type"],
                        str(last_packet_item),
                        int(last_packet_item["send_timestamp"]),
                        issuer_ratio_item[0],
                        1,
                        packet_id,
                        contract_args["contract"]["asset_peg"],
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0] * list(issuer_ratio_item[1].keys())[0], 4),
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0] * list(issuer_ratio_item[1].keys())[0], 4),
                        list(issuer_ratio_item[1].keys())[0],
                        "change_generated",
                        list(quote_price_item[issuer_ratio_item[0]].values())[0],
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (
                        last_packet_item["transaction_type"],
                        str(last_packet_item),
                        int(last_packet_item["send_timestamp"]),
                        issuer_ratio_item[0],
                        1,
                        packet_id,
                        contract_args["contract"]["asset_peg"],
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0] * list(issuer_ratio_item[1].keys())[0], 4),
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0] * list(issuer_ratio_item[1].keys())[0], 4),
                        list(issuer_ratio_item[1].keys())[0],
                        "change_generated",
                        list(quote_price_item[issuer_ratio_item[0]].values())[0],)

                # For accounting purposes, every transaction must create a line, we add the newly sold/disabled transactions so as to reflect the transaction_value amounts
                for issuer_ratio_item in iter(issuer_lock_ratio_amounts['value_fill'].items()):  # We use value_fill key to limit the number of loops to to the correct number of transactions to add to the db, we could have used any other key as they are all consistently equal in structure.
                    # Generate a contract for the receiving user
                    db_insert = storage.Contracts(
                        last_item[0],
                        issuer_ratio_item[0],
                        hashlib_jua.sha256(str(last_item[2]).encode("utf-8")).hexdigest(),
                        last_item[3],
                        issuer_ratio_item[0],
                        last_item[5],
                        round(list(value_remainder_items[issuer_ratio_item[0]].values())[0], 2),
                        10000,  # 10000 days as it's now a perpetual contract
                        list(issuer_ratio_item[1].keys())[0],
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0], 4),
                        round(list(value_items[issuer_ratio_item[0]].values())[0], 2),
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        -1,
                        packet_id,
                        list(quote_price_item[issuer_ratio_item[0]].values())[0],
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

                    self.response_hash += (last_item[0],
                        issuer_ratio_item[0],
                        hashlib_jua.sha256(str(last_item[2]).encode("utf-8")).hexdigest(),
                        last_item[3],
                        issuer_ratio_item[0],
                        last_item[5],
                        round(list(value_remainder_items[issuer_ratio_item[0]].values())[0], 2),
                        10000,  # 10000 days as it's now a perpetual contract
                        list(issuer_ratio_item[1].keys())[0],
                        round(list(amount_lock_items[issuer_ratio_item[0]].values())[0], 4),
                        round(list(value_items[issuer_ratio_item[0]].values())[0], 2),
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        -1,
                        packet_id,
                        list(quote_price_item[issuer_ratio_item[0]].values())[0],)

                # Add to the accumulation tables for storage.TotalValueLocked
                obj_count_________ = 0
                loop_obj_________ = list(issuer_lock_ratio_amounts.items())
                total_contracts_summary_amount = 0
                total_reserves_summary_amount = 0
                for issuer_ratio_item in iter(issuer_lock_ratio_amounts['value_fill'].items()):  # We use value_fill key to limit the number of loops to to the correct number of transactions to add to the db, we could have used any other key as they are all consistently equal in structure.
                    total_amount_ = (
                        storage.db.session.query(
                            storage.TotalValueLocked.total_contracted_amount,
                            storage.TotalValueLocked.total_reserves_amount,
                        )
                        .filter(
                            storage.TotalValueLocked.lock_ratio == list(issuer_ratio_item[1].keys())[0],
                            storage.TotalValueLocked.asset_peg
                            == contract_args["contract"]["asset_peg"],
                        )
                        .first()
                    )
                    loop_obj_________[obj_count_________] = (
                        storage.db.session.query(storage.TotalValueLocked)
                        .filter(
                            storage.TotalValueLocked.lock_ratio == list(issuer_ratio_item[1].keys())[0],
                            storage.TotalValueLocked.asset_peg
                            == contract_args["contract"]["asset_peg"],
                        )
                        .first()
                    )
                    if loop_obj_________[obj_count_________] != None:
                        loop_obj_________[
                            obj_count_________
                        ].total_contracted_amount = round(
                            float(total_amount_[0]) - round(list(amount_lock_items[issuer_ratio_item[0]].values())[0], 4
                        ), 4)
                        loop_obj_________[obj_count_________].total_reserves_amount = round(
                            float(total_amount_[1]) + round(list(amount_lock_items[issuer_ratio_item[0]].values())[0] * list(issuer_ratio_item[1].keys())[0], 4
                        ), 4)
                        loop_obj_________[
                            obj_count_________
                        ].updated = datetime_jua.datetime.utcnow().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        obj_count_________ += 1
                        total_contracts_summary_amount += round(list(amount_lock_items[issuer_ratio_item[0]].values())[0], 4
                        )
                        total_reserves_summary_amount += round(list(amount_lock_items[issuer_ratio_item[0]].values())[0] * list(issuer_ratio_item[1].keys())[0], 4
                        )

                        update_based_changes__ = db.session.query(storage.TotalValueLocked.total_contracted_amount, storage.TotalValueLocked.total_reserves_amount).order_by(storage.TotalValueLocked.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                        self.response_hash += (update_based_changes__,)
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "contract_response_hash": str(),
                            "content": "Could not access TotalValueLocked.",
                            "requisites": {},
                        }

                # Increment Config values
                config = (
                    storage.db.session.query(
                        storage.Config.withdrawal_datetime_id,
                        storage.Config.total_contracted_amount,
                        storage.Config.total_min_operational_value,
                        storage.Config.total_reserves_amount,
                        storage.Config.fee_rate,
                    )
                    .filter(
                        storage.Config.asset_peg == contract_args["contract"]["asset_peg"]
                    )
                    .first()
                )
    
                if config != None:
                    update = (
                        storage.db.session.query(storage.Config)
                        .filter(
                            storage.Config.asset_peg
                            == contract_args["contract"]["asset_peg"]
                        )
                        .first()
                    )
                    update.total_contracted_amount = round(float(config[1]) - total_contracts_summary_amount, 4
                    )
                    update.total_reserves_amount = round(float(config[3]) + total_reserves_summary_amount, 4
                    )
                    # total_min_operational_value is not calculated here as it was never removed when the reserves were locked
                    update.updated = datetime_jua.datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
    
                    update_based_changes__ = db.session.query(storage.Config.total_contracted_amount, storage.Config.total_reserves_amount).order_by(storage.Config.id.asc()).all()  # For performance, for update changes, we do not pull the entire database but only what we changed in this instance which is sufficient for the purposes of checking consistency
                    self.response_hash += (update_based_changes__,)
    
                    # save the response hash
                    contract_response_hash = hashlib_jua.sha256(str(self.response_hash).encode("utf-8")).hexdigest()
    
                    return {
                        "status": "complete",
                        "contract_response_code": 1,
                        "contract_response_hash": contract_response_hash,
                        "content": "Contract sold back into owners reserves.",
                        "requisites": {},
                    }
                else:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "contract_response_hash": str(),
                        "content": "Could not access Config.",
                        "requisites": {},
                    }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "contract_response_hash": str(),
                    "content": "Reserves contract not found",
                    "requisites": requisites,
                }
