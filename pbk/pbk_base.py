import os
import argparse
from gima_common.setup_logging import set_log
from gima_common.koppeling_minio import MinioClient
from gima_common.secrets_tool import get_secrets, secrets
from gima_common.postgres_functions import PostgresClient
import pandas as pd
import logging
from validate_data import check_data


set_log()
secrets = get_secrets()

pg_client = PostgresClient( host=secrets['postgres-pbk-host']
                            , database=secrets['postgres-pbk-database']
                            , user=secrets['postgres-pbk-user']
                            , password=secrets['postgres-pbk-password']
                            , port=secrets['postgres-pbk-port']
                        )

def load_pbk_cbs(pbkcbs_file):
    """
    Function loads PBK_CBS excel-data into table PBK_INPUT
    Input: name datafile
    Output: None
    """
    
    logging.info(f"""begin met laden pbk_cbs, bestand: {pbkcbs_file}""")

    # 'PBK_Kadaster2021.xls'

    minio_client = MinioClient()
    file_data = minio_client.download_file('pbk', pbkcbs_file)
    df = pd.read_excel(file_data)

    # renamen met zinnige kolomnamen
    df=df.rename(columns=
                        {                               
                            "periode": "periodeid",                             
                            " periode": "periode",                           
                            " regio": "regioid",                            
                            " regio.1": "regio",                           
                            " soort": "soortid",                             
                            " soort.1": "soort",                           
                            " Index": "index_2015",                            
                            "Mutatie vorige periode": "mutatie_vorige_periode",            
                            "Mutatie zelfde periode vorig jaar": "mutatie_zelfde_periode_vorig_jaar",  
                            "Aantal verkochte woningen": "aantal_verkochte_woningen",          
                            "Gemiddelde verkoopprijs": "gemiddelde_verkoopprijs"  
                        }
                )

    # tabel PBK_INPUT opnieuw aanmaken
    drop_create_table(pg_client)
    # en vullen 
    df.to_sql('pbk_input', con=pg_client.get_bulk_connection(), if_exists='append')

    logging.info("klaar met laden pbk_cbs")


def load_pbk_basisbestand():
    """
    Function loads data from table PBK_INPUT into table INDICES
    """

    logging.info("begin met laden pbk_basisbestand")

    file = os.getcwd() + '/sql/qry_merge_pbk_cbs.sql'
    with open(file, 'r') as sqlFile:
        stmt = sqlFile.read()

    pg_client.execute_statement(stmt)
    logging.info("klaar met laden pbk_basisbestand")


def load_pbk_initial(datadump_file):
    """
    Function will create and initial fill table INDICES
    Condition is a data-dump in Excel from table 'indices' to be found in the L:\BasisBestanden\PBK  Prijsindex bestaande koopwoningen\PKB.mdb
    Input: name datafile
    Output: None
    """

    logging.info(f"""begin met laden pbk_initieel, bestand: {datadump_file}""")

    minio_client = MinioClient()
    file_data = minio_client.download_file('pbk', {datadump_file})
    df = pd.read_excel(file_data)
    # formatteren kolomnamen, lowercase, geen spaties
    df=df.rename(columns=str.lower)
    df.columns = df.columns.str.replace(' ','_')
    df=df.rename(columns={"index_2015=100": "index_2015"})        

    df.to_sql('indices', con=pg_client.get_bulk_connection(), if_exists='replace')
    file = os.getcwd() + '/sql/create_indices_table.sql'
    with open(file, 'r') as sqlFile:
        stmt = sqlFile.read()

    pg_client.execute_statement(stmt)

    logging.info("klaar met laden pbk_initieel")


def validate_data():
    """
    Function validates data in table PBK_INPUT with the use of function CHECK_DATA()
    If the data can not be validated, a text-message will be returned with error-information, otherwise
    an empty string will be returned
    Output: text
    """
    logging.info("begin met valideren CBS-data")

    res = check_data()
    if res != '':
        logging.error(f"""De aangeboden CBS-data is niet goed: {res}""" )
        raise ValueError(res)

    logging.info("klaar met valideren CBS-data")


def drop_create_table(pg_client):
    """
    This function executes script ./sql/create_pbkinput_table.sql in postgres database
    Input: postgres session
    Output: None
    """
    file = os.getcwd() + '/sql/create_pbkinput_table.sql'
    with open(file, 'r') as sqlFile:
        stmt = sqlFile.read()

    logging.info('drop and create (if not exists) table PBK_INPUT...')
    pg_client.execute_statement(stmt)



if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--executeFunction", help="function to run(DST) or determine ")
    # args = parser.parse_args()
    # start_proces(execute_function=args.executeFunction)
    # laden_pbk_initieel('dump_pbk202108.xlsx')
    load_pbk_cbs('PBK_Kadaster2021.xls')
    # validate_data()
    # load_pbk_basisbestand()


