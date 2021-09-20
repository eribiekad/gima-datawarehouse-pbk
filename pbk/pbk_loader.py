import argparse
from gima_common.setup_logging import set_log
from verrijking_aktes import verrijk_aktes
from nieuwbouw import bepaal_nieuwbouw
from sync_postgres import store_data
import logging

set_log()


def start_proces(execute_function, p_jaarmaand = None):
    if execute_function == "verrijk-aktes":
        logging.info("verrijk aktes door middel van textherkenning")
        verrijk_aktes()
    elif execute_function == "bepaal-nieuwbouw":
        logging.info("bepalen nieuwbouw")
        bepaal_nieuwbouw(p_jaarmaand)
    elif execute_function == "push-to-postgres":
        logging.info("push data to postgres")
        store_data(p_jaarmaand)
    else:
        logging.error(f"NIET BEKEND COMMANDO: '{execute_function}'")
        raise Exception


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executeFunction", help="function to run(DST) or determine ")
    parser.add_argument("--timeperiod", help="required year and month, default is None")
    args = parser.parse_args()
    start_proces(execute_function=args.executeFunction, p_jaarmaand=args.timeperiod)
