# -*- coding: utf-8 -*-

# *********************************************************************************************
# ----------------------------------- *** Core Settings *** -----------------------------------
# *********************************************************************************************

version_no = "1.1.0"  # Version of this contract
version_target = "1.0.1"  # The minimum Kujua version this contract supports
about = "This contract is used for the facilitation of movements of value from sender to receiver through the use of perpetual contracts."

contract_address = "sx117afb1bba0e117a39a77b9ef4314fe322575903e48c635532e8e252363142c1"  # Add your public address here
close_of_day_save = False  # This is what tells the blockchain to save the results hash in case you will need the results in future e.g typically the following day. At an extra fee.
requisites = {
    "save": 0,
    "contract_lock_status": 0,
    "current_15m": {"stakers_fee_rate": 0.001, "withdrawal_rate": 0.001},
    "previous_15m": {"stakers_fee_rate": 0.001, "withdrawal_rate": 0.001},
}  # Requirements that can verify that a contract execution will be successful or not, prior to when the execution occurs e.g. a fee charge required to execute the contract may need to be known prior to the end user executing the contract so that they do not spend any blockchain fee only for their contract fee to be declined by the smart contract. These values are saved by the node every 15 minutes if the 'save' parameter is set to 1. Only the parameter 'save' and 'contract_lock_status' are standard, the rest are only relevant to this specific contract

# *********************************************************************************************
# --------------------------------- *** end of core settings *** ------------------------------
# *********************************************************************************************

import sys as sys_jua

sys_jua.path.append("")  # Location of custom modules such as "storage"
import storage  # storage is a custom module. Because Kujua does not allow custom modules then one needs to run their own node when their custom module is a necessity in their contract. In this case we run this contract on our own node and distribute the results to the blockchain. This is because the blockchain does not store custom database data but only stores the most recent state of the contract. The contract will post this state on a periodic basis. Luckily running a node on Kujua is very cheap and easy and can run on a 4gb ram computer depending on how busy your contract gets.
import kujuaRelayChain.Client.functions as functions
from db_config import (
    HistoryLatest,
)  # Kujua allows importing standard tables such as History as read-only as this is where transactional data is stored. In this case we use from... as we are plugging in directly to the nodes read-only configuration.
import string as string_jua
import random as random_jua
import datetime as datetime_jua
import ast as ast_jua
import hashlib as hashlib_jua
import sqlalchemy as sqlalchemy_jua


class Definitions:
    global version_no, version_target, about

    def __init__(
        self,
        contract_address,
        close_of_day_save,
        requisites,
    ):
        self.version_no = version_no
        self.version_target = version_target
        self.about = about

        self.supported_currency_codes = ["KES", "ZAR", "ZMW", "NAD", "USD", "JUA"]
        self.contract_address = contract_address
        self.close_of_day_save = close_of_day_save
        self.requisites = requisites


class Contract(Definitions):
    def __int__(
        self,
        contract_address,
        close_of_day_save,
        requisites,
    ):
        super().__init__(
            contract_address,
            close_of_day_save,
            requisites,
        )

    def results(
        self,
        packet: dict,
        sequential_pos: int,
        sequential_hash: str,
        status: int,
        status_signature: str,
        recieved_datetime_id: int,
        current_gateway_decoded: str,
        _day: str,
        quoted_price: float,
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

                storage.db.session.commit()
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
                        "",
                        0,
                        0,
                        0,
                        0,
                        datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                    for x in range(3):
                        try:
                            storage.db.session.add(db_insert)
                            storage.db.session.commit()
                            break
                        except:
                            storage.db.session.rollback()
                            if x == 2:
                                raise Exception(
                                    "Could not access the database upon insert."
                                )

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

            for cur in self.supported_currency_codes:
                db_insert = storage.Config(
                    cur,
                    "19990101235959999999",
                    0,
                    "",
                    0,
                    0,
                    0,
                    0,
                    datetime_jua.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                )

                for x in range(3):
                    try:
                        storage.db.session.add(db_insert)
                        storage.db.session.commit()
                        break
                    except:
                        storage.db.session.rollback()
                        if x == 2:
                            raise Exception(
                                "Could not access the database upon insert."
                            )

            storage.db.session.commit()
            return {
                "status": "complete",
                "contract_response_code": 1,
                "content": "Tables created in database",
                "requisites": {},
            }

        # Send reserves contracts sent from one account to another (NOT JUANA). If transacting from one smart_contract account to another
        if contract_args["action"] == "transact_api":
            if contract_args != {}:
                # Ask the node to save the requisites if it does not have any saved for this contract
                if ("current_15m" not in iter(requisites.keys())) or (
                    "previous_15m" not in iter(requisites.keys())
                ):
                    requisites = {
                        "save": 1,
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
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Missing required parameter for requisites 'save', see documentation. Request to save parameter sent. Rerun last transaction.",
                        "requisites": requisites,
                    }

                # Get both the current and previous 15 minute's rates, if the current rate period has passed the contract will allow using the previous 15 minute's rate
                stakers_fee_rate_c = requisites["current_15m"]["stakers_fee_rate"]
                stakers_fee_rate_p = requisites["previous_15m"]["stakers_fee_rate"]

                if contract_args["contract"]["transaction_value"] <= 0:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Invalid amount transacted",
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
                        "content": "Cannot send to oneself",
                        "requisites": {},
                    }

                # Verify they have valid contracts and add up to see that the total being sent is enough to send the transaction_value
                last_item = None
                contracts__ = (
                    storage.db.session.query(
                        storage.Contracts.claimed_univ_sequential_hashes,
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
                        storage.Contracts.universal_sequential_hash,
                    )
                    .filter(
                        storage.Contracts.owner_address
                        == list(packet.values())[0]["sender_address"],
                        storage.Contracts.status == 1,
                        storage.Contracts.asset_peg
                        == contract_args["contract"]["asset_peg"],
                        storage.Contracts.universal_sequential_hash.in_(
                            contract_args["contract"]["claimed_univ_sequential_hashes"]
                        ),
                    )
                    .all()
                )

                if contracts__ == []:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "No active contracts found for sender {} and asset peg {} on these sequential hashes: {}".format(
                            list(packet.values())[0]["sender_address"],
                            contract_args["contract"]["asset_peg"],
                            contract_args["contract"]["claimed_univ_sequential_hashes"],
                        ),
                        "requisites": requisites,
                    }

                owner_fill_amount = 0
                disabled_sequential_hashes_list = []
                value_fill = 0
                for req in contracts__:
                    disabled_sequential_hashes_list.append(req[16])
                    if (value_fill + float(req[6])) >= contract_args["contract"][
                        "transaction_value"
                    ]:
                        value_fill = (value_fill + float(req[6])) - contract_args[
                            "contract"
                        ]["transaction_value"]
                        owner_fill_amount = owner_fill_amount + float(
                            req[9]
                        )  # - round(contract_args["contract"]["transaction_value"] / quoted_price, 4))
                        last_item = req
                        break

                    else:
                        value_fill += float(req[6])
                        owner_fill_amount += float(req[9])

                # Check if the sender sent enough contract value to split between the receiver and the sender's change at the current market price
                if owner_fill_amount < (
                    float(contract_args["contract"]["transaction_value"]) / quoted_price
                ):
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Insufficient contracts value receipt of {} JUA to transact {} {} at market price {}. Must send contracts worth at least {} JUA. Failed at conversion from asset peg value to Juana.".format(
                            owner_fill_amount,
                            contract_args["contract"]["transaction_value"],
                            contract_args["contract"]["asset_peg"],
                            quoted_price,
                            round(
                                float(contract_args["contract"]["transaction_value"])
                                / quoted_price,
                                4,
                            ),
                        ),
                        "requisites": {},
                    }

                if last_item == None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Unable to find required contract items",
                        "requisites": requisites,
                    }

                fee_received = 0
                for f in list(packet.values()):
                    if (f["transaction_type"] == "9") and (
                        f["receiver_address"] == last_item[4]
                    ):
                        fee_received += float(f["note_balance"])

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

                # Generate a contract for the receiving user
                db_insert = storage.Contracts(
                    last_item[0],
                    contract_args["contract"]["owner_address"],
                    hashlib_jua.sha256(str(last_item[2]).encode("utf-8")).hexdigest(),
                    last_item[3],
                    last_item[4],
                    last_item[5],
                    round(float(contract_args["contract"]["transaction_value"]), 2),
                    10000,  # 10000 days as it's now a perpetual contract
                    last_item[8],
                    round(
                        owner_fill_amount
                        - (
                            owner_fill_amount
                            - (
                                float(contract_args["contract"]["transaction_value"])
                                / quoted_price
                            )
                        ),
                        4,
                    ),
                    round(float(contract_args["contract"]["transaction_value"]), 2),
                    stakers_fee_rate_c,
                    int(last_item[11]),
                    last_item[12],
                    "perpetual",
                    str(contract_args),
                    1,
                    sequential_hash,
                    1,
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

                # Generate the contract change if there is any
                if value_fill > 0:
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
                            value_fill,
                            2,
                        ),
                        10000,  # 10000 days as it's now a perpetual contract
                        last_item[8],
                        round(
                            owner_fill_amount
                            - (
                                float(contract_args["contract"]["transaction_value"])
                                / quoted_price
                            ),
                            4,
                        ),
                        round(float(contract_args["contract"]["transaction_value"]), 2),
                        stakers_fee_rate_c,
                        int(last_item[11]),
                        last_item[12],
                        "perpetual",
                        str(contract_args),
                        1,
                        sequential_hash,
                        1,
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
                            storage.Contracts.universal_sequential_hash == ds,
                            storage.Contracts.owner_address
                            == list(packet.values())[0][
                                "sender_address"
                            ],  # This is included as more than one contract can share the same universal_sequential_hash after splitting for change
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
                            "content": "No contract available for your address to use for this transaction. Please generate a contract first or have a third party generate one for you.",
                            "requisites": requisites,
                        }

                storage.db.session.commit()
                return {
                    "status": "complete",
                    "contract_response_code": 1,
                    "content": "Transaction complete",
                    "requisites": requisites,
                }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "content": "Reserves contract not found",
                    "requisites": requisites,
                }

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
                        storage.ReserveTransactions.universal_sequential_hash
                        == sequential_hash,
                    )
                    .first()
                )

                if check_exist != None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Coin already exists",
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
                            "content": "Missing transaction in packet.",
                            "requisites": {},
                        }

                    db_insert = storage.ReserveTransactions(
                        int(last_item["coin_head_id"]),
                        last_item["transaction_type"],
                        str(last_item),
                        int(last_item["send_timestamp"]),
                        last_item["sender_address"],
                        status,
                        status_signature,
                        sequential_pos,
                        sequential_hash,
                        contract_args["contract"]["asset_peg"],
                        round(float(last_item["transaction_type_balance"]), 4),
                        contract_args["contract"]["lock_ratio"],
                        "sender_generated",
                        1,
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
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "content": "Could not access table TotalValueLocked.",
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

                        storage.db.session.commit()
                        return {
                            "status": "complete",
                            "contract_response_code": 1,
                            "content": "Locked reserves for: {}".format(contract_args),
                            "requisites": {},
                        }
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "content": "Could not access table Config.",
                            "requisites": {},
                        }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
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
                        storage.ReserveTransactions.universal_sequential_hash
                        == sequential_hash,
                    )
                    .first()
                )

                if check_exist != None:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Coin already exists",
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
                        "content": "Invalid asset peg supplied",
                        "requisites": {},
                    }

                else:
                    # Check that the issuer_address has enough reserves to create this contract at the given ratio level. The issuer cannot mix different ratio levels to fulfill a single order and therefore must have enough to cover the single order at the given ratio level, that feature will be available in future when contracts are created without the issuer granting them out.
                    owner_reserves = (
                        storage.db.session.query(
                            storage.ReserveTransactions.coin_id,
                            storage.ReserveTransactions.transaction_type,
                            storage.ReserveTransactions.coin,
                            storage.ReserveTransactions.sent_date_id,
                            storage.ReserveTransactions.issuer_address,
                            storage.ReserveTransactions.status,
                            storage.ReserveTransactions.status_signature,
                            storage.ReserveTransactions.universal_sequential_pos,
                            storage.ReserveTransactions.universal_sequential_hash,
                            storage.ReserveTransactions.amount,
                            storage.ReserveTransactions.lock_ratio,
                            storage.ReserveTransactions.type,
                            storage.ReserveTransactions.asset_peg,
                        )
                        .filter(
                            storage.ReserveTransactions.issuer_address
                            == list(packet.values())[0]["sender_address"],
                            storage.ReserveTransactions.asset_peg
                            == contract_args["contract"]["asset_peg"],
                            storage.ReserveTransactions.status == 1,
                            storage.ReserveTransactions.amount > 0,
                            storage.ReserveTransactions.universal_sequential_hash.in_(
                                contract_args["contract"][
                                    "claimed_univ_sequential_hashes"
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
                            "content": "No reserves to create contract found for reserves sequential hashes: {}".format(
                                contract_args["contract"][
                                    "claimed_univ_sequential_hashes"
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
                                            "claimed_univ_sequential_hashes"
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
                                    sequential_hash,
                                    1,
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

                                # Generate reserves change if any and disable the used reserves contracts
                                remaining_reserves_fill = (
                                    total_issuer_reserves  # This becomes the change
                                )
                                last_item = None

                                remaining_fill = round(
                                    (
                                        float(contract_args["contract"]["value"])
                                        / quoted_price
                                    ),
                                    4,
                                )

                                # Notice the obj_count is not placed here as it may get reused and cause and error as its inside another loop
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
                                    (
                                        float(contract_args["contract"]["value"])
                                        / quoted_price
                                    ),
                                    4,
                                )

                                # Put the change back into the reserves
                                if remaining_reserves_fill > 0:
                                    db_insert = storage.ReserveTransactions(
                                        last_item[0],
                                        last_item[1],
                                        last_item[2],
                                        int(last_item[3]),
                                        last_item[4],
                                        status,
                                        status_signature,
                                        sequential_pos,
                                        sequential_hash,
                                        last_item[12],
                                        round(float(remaining_reserves_fill), 4),
                                        last_item[10],
                                        "change_generated",
                                        1,
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
                                            storage.ReserveTransactions.universal_sequential_hash
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
                                else:
                                    storage.db.session.rollback()
                                    return {
                                        "status": "error",
                                        "contract_response_code": 0,
                                        "content": "Could not access table TotalValueLocked.",
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

                                    storage.db.session.commit()
                                    return {
                                        "status": "complete",
                                        "contract_response_code": 1,
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
                                        "content": "Could not access table Config.",
                                        "requisites": {},
                                    }
                            else:
                                storage.db.session.rollback()
                                return {
                                    "status": "error",
                                    "contract_response_code": 0,
                                    "content": "Received an empty contract_args argument",
                                    "requisites": {},
                                }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
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
                        "content": "Invalid reserves amount to withdraw",
                        "requisites": {"contract_lock_status": 0},
                    }

                # Verify they have a valid reserve
                owner_reserves = (
                    storage.db.session.query(
                        storage.ReserveTransactions.coin_id,
                        storage.ReserveTransactions.transaction_type,
                        storage.ReserveTransactions.coin,
                        storage.ReserveTransactions.sent_date_id,
                        storage.ReserveTransactions.issuer_address,
                        storage.ReserveTransactions.status,
                        storage.ReserveTransactions.status_signature,
                        storage.ReserveTransactions.universal_sequential_pos,
                        storage.ReserveTransactions.universal_sequential_hash,
                        storage.ReserveTransactions.amount,
                        storage.ReserveTransactions.lock_ratio,
                        storage.ReserveTransactions.type,
                        storage.ReserveTransactions.quote_price,
                        storage.ReserveTransactions.asset_peg,
                    )
                    .filter(
                        storage.ReserveTransactions.status == 1,
                        storage.ReserveTransactions.amount > 0,
                        storage.ReserveTransactions.issuer_address
                        == list(packet.values())[0]["sender_address"],
                    )
                    .all()
                )

                if owner_reserves == []:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Reserves not found for {}".format(
                            list(packet.values())[0]["sender_address"]
                        ),
                        "requisites": {"contract_lock_status": 0},
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
                                    "content": "Insufficient funds to perform action. You can only withdraw to the value of {} subject to there being enough liquidity above 100% collateralisation.".format(
                                        total_available_reserves
                                    ),
                                    "requisites": {"contract_lock_status": 0},
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
                        "content": "Your withdrawal order date cannot be less than today or more than {}".format(
                            (
                                datetime_jua.datetime.utcnow()
                                + datetime_jua.timedelta(days=3)
                            ).strftime("%Y%m%d")
                        ),
                        "requisites": {"contract_lock_status": 0},
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
                    float(contract_args["withdraw_reserves_amount"]),
                    0,
                    1,
                    contract_args["asset_peg"],
                    sequential_hash,
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

                # Notice the obj_count is not placed here as it may get reused and cause and error as its inside another loop
                for req in owner_reserves:
                    if remaining_fill <= 0:
                        last_item = req
                        break

                    elif remaining_reserves_fill >= remaining_fill:
                        # Then we know we are on the last loop
                        last_item = req
                        disabled_sequential_hashes_list.append(req[8])
                        break
                    else:
                        remaining_fill -= float(req[9])
                        disabled_sequential_hashes_list.append(req[8])

                    if (req == owner_reserves[-1]) and (remaining_fill > 0):
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
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
                        last_item[0],
                        last_item[1],
                        last_item[2],
                        int(last_item[3]),
                        last_item[4],
                        status,
                        status_signature,
                        sequential_pos,
                        sequential_hash,
                        last_item[13],
                        round(float(remaining_reserves_fill), 4),
                        last_item[10],
                        "change_generated",
                        1,
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
                            storage.ReserveTransactions.universal_sequential_hash == ds,
                        )
                        .first()
                    )
                    loop_obj_____________[
                        obj_count_____________
                    ].status = (
                        -1
                    )  # We permanently disable because when the generated contract is sold back into the reserves it will create a new record for reserves
                    obj_count_____________ += 1

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
                    else:
                        storage.db.session.rollback()
                        return {
                            "status": "error",
                            "contract_response_code": 0,
                            "content": "Could not access table TotalValueLocked.",
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
                            sum(list(ratio_level_amounts.values()))
                            - (sum(list(ratio_level_amounts.values())) - remaining_fill)
                        ),
                        4,
                    )
                    update.total_min_operational_value = round(
                        float(config[2]) - (round(min_operational_value, 4)), 2
                    )
                    update.updated = datetime_jua.datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                else:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Could not access table Config.",
                        "requisites": {"contract_lock_status": 0},
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
                    storage.db.session.commit()  # We allow saving here while keeping the order with an active status as we will process these orders at close of day
                    return {
                        "status": "complete",
                        "contract_response_code": 1,
                        "content": "Reserves below operational levels to allow withdrawals, this order has been queued to be processed later today at close of day 23:59:59 UTC should there be an excess in reserves above 100% collateralisation.",
                        "requisites": {
                            "contract_lock_status": 0
                        },  # We do not change the lock status as it will be changed at close of day
                    }

                else:
                    # Disable the order and send it to the user's wallet
                    update = (
                        storage.db.session.query(storage.WithdrawalOrders)
                        .filter(
                            storage.WithdrawalOrders.sequential_hash == sequential_hash
                        )
                        .first()
                    )
                    if update != None:
                        update.status = -1

                        storage.db.session.commit()
                        return {
                            "status": "complete",
                            "contract_response_code": 1,
                            "content": "Withdrawal order sent to wallet {}, amount {}".format(
                                list(packet.values())[0]["sender_address"],
                                float(contract_args["withdraw_reserves_amount"]),
                            ),
                            "requisites": {"contract_lock_status": 1},
                        }

            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "content": "Reserves contract not found",
                    "requisites": {"contract_lock_status": 0},
                }

        # Sell a contract back to reserves in exchange for Juana
        elif contract_args["action"] == "sell_contract":
            owner_contract = (
                storage.db.session.query(
                    storage.Contracts.claimed_univ_sequential_hashes,
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
                    storage.Contracts.universal_sequential_hash,
                )
                .filter(
                    storage.Contracts.owner_address
                    == list(packet.values())[0]["sender_address"],
                    storage.Contracts.status == 1,
                    storage.Contracts.asset_peg
                    == contract_args["contract"]["asset_peg"],
                    storage.Contracts.universal_sequential_hash.in_(
                        contract_args["contract"]["claimed_univ_sequential_hashes"]
                    ),
                )
                .all()
            )

            if owner_contract == []:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "content": "Address {} does not own any {} contracts to sell".format(
                        list(packet.values())[0]["sender_address"],
                        contract_args["contract"]["asset_peg"],
                    ),
                    "requisites": {},
                }

            available_amount = 0
            issuer_address = None
            for contr in owner_contract:
                available_amount += float(contr[9])
                if available_amount >= round(
                    float(contract_args["contract"]["amount"]), 4
                ):
                    issuer_address = contr[4]
                    break

            if available_amount < round(float(contract_args["contract"]["amount"]), 4):
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "content": "Insufficient funds of {} to sell contracts worth: {}".format(
                        available_amount, contract_args["contract"]["amount"]
                    ),
                    "requisites": {},
                }

            remaining_fill = round(float(contract_args["contract"]["amount"]), 4)
            reserve_returns_fill = (
                {}
            )  # {sequential_hash: {lock_ratio: amount}}  # Helps put back the deducted amount to their respective sequential_hash
            lock_ratio_amounts = (
                {}
            )  # Format: {lock_ratio: amount}  # For the TotalValueLocked table
            sold_contracts = []
            for contr in owner_contract:
                sold_contracts.append(contr[16])

                if (remaining_fill - float(contr[9])) <= float(
                    contract_args["contract"]["amount"]
                ):
                    remaining_fill = round(float(contr[9]) - remaining_fill, 4)

                    if contr[16] not in list(reserve_returns_fill.keys()):
                        reserve_returns_fill[contr[16]] = {contr[8]: 0}

                    if contr[16] in iter(reserve_returns_fill.keys()):
                        if contr[8] not in list(reserve_returns_fill[contr[16]].keys()):
                            reserve_returns_fill[contr[16]][contr[8]] = 0

                    if contr[8] not in list(lock_ratio_amounts.keys()):
                        lock_ratio_amounts[contr[8]] = 0

                    reserve_returns_fill[contr[16]][contr[8]] += (
                        float(contr[9]) - remaining_fill
                    )  # += because sequential hashes are not unique in the reserves table and we might have already added to this current sequential hash
                    lock_ratio_amounts[contr[8]] += float(contr[9]) - remaining_fill

                    # Generate the change if any, using the last contract values
                    if remaining_fill > 0:
                        db_insert__ = storage.Contracts(
                            contr[0],
                            contr[1],
                            hashlib_jua.sha256(
                                str(contr[2]).encode("utf-8")
                            ).hexdigest(),
                            contr[3],
                            contr[4],
                            contr[5],
                            round(
                                float(contract_args["contract"]["amount"])
                                * quoted_price,
                                2,
                            ),
                            10000,  # 10000 days as it's a perpetual contract
                            contr[8],
                            remaining_fill,
                            round(float(contract_args["contract"]["amount"]), 4),
                            0,
                            int(contr[11]),
                            contr[12],
                            "perpetual",
                            str(contract_args),
                            1,
                            sequential_hash,
                            1,
                            datetime_jua.datetime.utcnow().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
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

                    break
                else:
                    remaining_fill -= float(contr[9])

                    if contr[16] not in iter(reserve_returns_fill.keys()):
                        reserve_returns_fill[contr[16]] = {contr[8]: 0}

                    if contr[16] in iter(reserve_returns_fill.keys()):
                        if contr[8] not in iter(reserve_returns_fill[contr[16]].keys()):
                            reserve_returns_fill[contr[16]][contr[8]] = 0

                    if contr[8] not in iter(lock_ratio_amounts.keys()):
                        lock_ratio_amounts[contr[8]] = 0

                    reserve_returns_fill[contr[16]][contr[8]] += float(
                        contr[9]
                    )  # += because sequential hashes are not unique in the reserves table and we might have already added to this current sequential hash
                    lock_ratio_amounts[contr[8]] += float(contr[9])

            # Disable the sold contracts
            obj_count___________ = 0
            loop_obj___________ = sold_contracts[:]
            for ds in sold_contracts:
                loop_obj___________[obj_count___________] = (
                    storage.db.session.query(storage.Contracts)
                    .filter(
                        storage.Contracts.asset_peg
                        == contract_args["contract"]["asset_peg"],
                        storage.Contracts.status == 1,
                        storage.Contracts.universal_sequential_hash == ds,
                        storage.Contracts.owner_address
                        == list(packet.values())[0][
                            "sender_address"
                        ],  # This is included as more than one contract can share the same universal_sequential_hash after splitting for change
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
                        "content": "No contract available for your address to use for this transaction. Please generate a contract first or have a third party generate one for you.",
                        "requisites": requisites,
                    }

            # Return reserves back to the same issuer at the borrowed ratio level. We insert using the newly received transaction packet details instead of update existing reserve items as multiple reserves may have been disabled to create a single contract
            last_item = None
            for t in iter(packet.values()):
                if t["transaction_type"] == "1":
                    last_item = t
                    break

            if last_item == None:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "content": "Missing transaction in packet.",
                    "requisites": {},
                }

            for contr_r in iter(reserve_returns_fill.items()):
                db_insert = storage.ReserveTransactions(
                    int(last_item["coin_head_id"]),
                    last_item["transaction_type"],
                    str(last_item),
                    int(last_item["send_timestamp"]),
                    issuer_address,
                    status,
                    status_signature,
                    sequential_pos,
                    sequential_hash,
                    contract_args["contract"]["asset_peg"],
                    round(
                        list(contr_r[1].items())[0][1] * list(contr_r[1].items())[0][0],
                        4,
                    ),
                    list(contr_r[1].items())[0][0],
                    "change_generated",
                    1,
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

            # Add to the accumulation tables for storage.TotalValueLocked
            obj_count_________ = 0
            loop_obj_________ = list(lock_ratio_amounts.items())
            for contr_r in iter(lock_ratio_amounts.items()):
                total_amount_ = (
                    storage.db.session.query(
                        storage.TotalValueLocked.total_contracted_amount,
                        storage.TotalValueLocked.total_reserves_amount,
                    )
                    .filter(
                        storage.TotalValueLocked.lock_ratio == contr_r[0],
                        storage.TotalValueLocked.asset_peg
                        == contract_args["contract"]["asset_peg"],
                    )
                    .first()
                )
                loop_obj_________[obj_count_________] = (
                    storage.db.session.query(storage.TotalValueLocked)
                    .filter(
                        storage.TotalValueLocked.lock_ratio == contr_r[0],
                        storage.TotalValueLocked.asset_peg
                        == contract_args["contract"]["asset_peg"],
                    )
                    .first()
                )
                if loop_obj_________[obj_count_________] != None:
                    loop_obj_________[
                        obj_count_________
                    ].total_contracted_amount = round(
                        float(total_amount_[0]) - contr_r[1], 4
                    )
                    loop_obj_________[obj_count_________].total_reserves_amount = round(
                        float(total_amount_[1]) + contr_r[1], 4
                    )
                    loop_obj_________[
                        obj_count_________
                    ].updated = datetime_jua.datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    obj_count_________ += 1
                else:
                    storage.db.session.rollback()
                    return {
                        "status": "error",
                        "contract_response_code": 0,
                        "content": "Could not access table TotalValueLocked.",
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
                update.total_contracted_amount = round(
                    float(config[1]) - float(contract_args["contract"]["amount"]), 4
                )
                update.total_reserves_amount = round(
                    float(config[3]) + float(contract_args["contract"]["amount"]), 4
                )
                update.updated = datetime_jua.datetime.utcnow().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                storage.db.session.commit()
                return {
                    "status": "complete",
                    "contract_response_code": 1,
                    "content": "Contract sold back into owners reserves.",
                    "requisites": {},
                }
            else:
                storage.db.session.rollback()
                return {
                    "status": "error",
                    "contract_response_code": 0,
                    "content": "Could not access Config table.",
                    "requisites": {},
                }


# Optional: processes (cron jobs) to run upon the close of day, see whitepaper for details on what the close of day is
class CloseOfDay(Definitions):
    def __int__(
        self,
        contract_address,
        close_of_day_save,
        requisites,
    ):
        super().__init__(
            contract_address,
            close_of_day_save,
            requisites,
        )

    def withdrawals(self, current_gateway_decoded: str, _day: str, quoted_price: float):
        global contract_address
        # Process withdrawal orders for the day

        # We do not filter by date as there may be orders from the previous days to process first
        withdraw_orders = (
            storage.db.session.query(
                storage.WithdrawalOrders.owner_address,
                storage.WithdrawalOrders.order_id,
                storage.WithdrawalOrders.fill_amount,
                storage.WithdrawalOrders.amount,
                storage.WithdrawalOrders.asset_peg,
                storage.WithdrawalOrders.sequential_hash,
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
                    "content": "All asset reserves below operational levels to allow withdrawals, waiting for more reserves to be added before processing withdrawals.",
                    "requisites": {},
                }

            withdrawal_orders = {}  # Format: {sequential_hash: [amount, asset]}
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
                    .filter(storage.WithdrawalOrders.sequential_hash == req[0])
                    .first()
                )
                loop_obj_______________[obj_count_______________].status = -1
                obj_count_______________ += 1

            storage.db.session.commit()
            return {
                "status": "complete",
                "contract_response_code": 1 if (sequential_hashes != []) else 0,
                "content": "Previously delayed withdrawals have been processed for the day {} for the available excess reserves.".format(
                    _day
                ),
                "requisites": {
                    "save": 1 if (sequential_hashes != []) else 0,
                    "contract_unlock_hashes": sequential_hashes,
                },
            }
        else:
            return {
                "status": "complete",
                "contract_response_code": 1,
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
            storage.db.session.commit()
            return {
                "status": "complete",
                "contract_response_code": 1,
                "content": "Close of day contract process finished with response: {}".format(
                    withdrawals["content"]
                ),
                "requisites": {},
            }


contract = Contract(
    contract_address=contract_address,
    close_of_day_save=close_of_day_save,
    requisites=requisites,
)

closeofday = CloseOfDay(
    contract_address=contract_address,
    close_of_day_save=close_of_day_save,
    requisites=requisites,
)
