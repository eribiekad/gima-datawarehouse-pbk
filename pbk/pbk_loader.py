import os
import argparse
from gima_common.setup_logging import set_log
import logging
from pbk_base import load_pbk_basisbestand, load_pbk_cbs, validate_data

set_log()


def start_proces(execute_function):
    if execute_function == "laden_pbk_cbs":
        logging.info("laden pbk_cbs")
        load_pbk_cbs()
    elif execute_function == "valideren_pbk_cbs":
        logging.info("valideren pbk_cbs")
        validate_data()
    elif execute_function == "laden_pbk_basisbestand":
        logging.info("laden pbk_basisbestand")
        load_pbk_basisbestand()
    else:
        logging.error(f"NIET BEKEND COMMANDO: '{execute_function}'")
        raise Exception


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executeFunction", help="function to run(DST) or determine ")
    args = parser.parse_args()
    start_proces(execute_function=args.executeFunction)


