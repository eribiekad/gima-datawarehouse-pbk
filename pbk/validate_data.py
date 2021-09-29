from gima_common.setup_logging import set_log
from gima_common.secrets_tool import get_secrets, secrets
from gima_common.postgres_functions import PostgresClient
from gima_common.oracle_functions import OracleClient
import pandas as pd
import logging

set_log()
secrets = get_secrets()

pg_client = PostgresClient( host=secrets['postgres-pbk-host']
                        , database=secrets['postgres-pbk-database']
                        , user=secrets['postgres-pbk-user']
                        , password=secrets['postgres-pbk-password']
                        , port=secrets['postgres-pbk-port']
                    )

ora_client = OracleClient(   service=secrets['oracle-pbk-service']
                            , user=secrets['oracle-pbk-user']
                            , password=secrets['oracle-pbk-password']
                        )  


def check_data():
    """
    Deze functie geeft een lege string terug indien aan alle controles is voldaan.
    Is een fout geconstateerd, dan wordt een string met de betreffende Periode, Gebied en (optioneel) Soort teruggegeven.
    """

    fn_return_value = ''

    # Bepaal de meest recente maand in de invoer
    sql = f"""select max(pbk_input.periodeid)
                from pbk_input
                where pbk_input.periodeid not like '%13'
                  and pbk_input.periodeid not like '%14'
                  and pbk_input.periodeid not like '%15'
                  and pbk_input.periodeid not like '%16'
                  and pbk_input.periodeid not like '%00'
                  and pbk_input.index_2015 is not null"""
                
    res = pg_client.get_data(sql)
    maandmax_pbkinput = res[0][0]
    logging.info(f"""maand-max pbk_cbs: {maandmax_pbkinput}""")

    # De controles vinden plaats tegen de OBK_OV03_UITVOER, deze moet zijn ingelezen voor de meest recente maand
    sql = f"""select max(periode)
              from gma_own.obk_ov03_uitvoer"""

    res = ora_client.get_data(sql)         
    maandmax_ov03 = res[0][0]    
    logging.info(f"""maand-max OV03: {maandmax_ov03}""")

    if maandmax_pbkinput > maandmax_ov03:
        fn_return_value = f"""OBK_OV03_UITVOER nog niet ingelezen voor periode {maandmax_pbkinput}"""
        return fn_return_value
    
    # Controleer de gegevens van de meest recente maand
    foutgebied = fetch_data('', '', maandmax_pbkinput, '', '')
    if foutgebied != '':
        fn_return_value = f"""CBS_PBK bestand heeft een fout in periode: {maandmax_pbkinput} en regio: {foutgebied}"""
        return fn_return_value
    
    # Bepaal het meest recente kwartaal in de invoer
    sql = f"""  select max(periodeid)
                from pbk_input
                where (periodeid like '%13'
                    or periodeid like '%14'
                    or periodeid like '%15'
                    or periodeid like '%16')
                and periodeid not like '%00'
                and index_2015 is not null
            """
    res = pg_client.get_data(sql)

    if res[0][0] is None:
        kwartaalmax = maandmax_pbkinput
    else:
        kwartaalmax = res[0][0]

    logging.info(f"""kwartaal-max pbk_cbs: {kwartaalmax}""")    
    
    # Kwartaalgegevens alleen controleren indien de meest recente maand de laatste maand
    # van het meest recente kwartaal is
    if ( 
            (kwartaalmax[-2:] == '13' and maandmax_pbkinput[-2:] == '03') or 
            (kwartaalmax[-2:] == '14' and maandmax_pbkinput[-2:] == '06') or 
            (kwartaalmax[-2:] == '15' and maandmax_pbkinput[-2:] == '09') or 
            (kwartaalmax[-2:] == '16' and maandmax_pbkinput[-2:] == '12') 
        ):

        # Bepaal de minst recente maand in het meest recente kwartaal
        if kwartaalmax[-2:] == '13':
            maandmin = kwartaalmax[:4] + '01'
        if kwartaalmax[-2:] == '14':
            maandmin = kwartaalmax[:4] + '04'
        if kwartaalmax[-2:] == '15':
            maandmin = kwartaalmax[:4] + '07'
        if kwartaalmax[-2:] == '16':
            maandmin = kwartaalmax[:4] + '10'

        # Controleer de landelijke gegevens
        for soort in range(1, 8):
            foutgebied = fetch_data(kwartaalmax, maandmin, maandmax_pbkinput, '', soort)
            if foutgebied != '':
                fn_return_value = f"""CBS_PBK bestand heeft een fout in periode: {kwartaalmax} en regio: {foutgebied} en soort: {soort} """
                return fn_return_value

        # Controleer de gegevens van grote steden met ge-/misbruik van soort voor sturing
        for geb in range(1, 4):
            foutgebied = fetch_data(kwartaalmax, maandmin, maandmax_pbkinput, geb, 9)
            if foutgebied != '':
                fn_return_value = f"""CBS_PBK bestand heeft een fout in periode: {kwartaalmax} en regio: {foutgebied} """
                return fn_return_value

        # Controleer de gegevens van provincies met ge-/misbruik van soort voor sturing
        for geb in range(20, 31):
            foutgebied = fetch_data(kwartaalmax, maandmin, maandmax_pbkinput, geb, 10)
            if foutgebied != '':
                fn_return_value =  f"""CBS_PBK bestand heeft een fout in periode: {kwartaalmax} en regio: {foutgebied} """
                return fn_return_value

        # Controleer de gegevens van regio's met ge-/misbruik van soort voor sturing
        for geb in range(1, 4):
            foutgebied = fetch_data(kwartaalmax, maandmin, maandmax_pbkinput, geb, 11)
            if foutgebied != '':
                fn_return_value =  f"""CBS_PBK bestand heeft een fout in periode: {kwartaalmax} en regio: {foutgebied} """
                return fn_return_value

    return fn_return_value


def fetch_data(kwartaal, minperiode, maxperiode, gebied, soort):
    """
    Deze functie haalt gegevens per periode, gebied en (optioneel) soort op uit pbk_input en de obk_ov03_uitvoer tabel.
    De opgehaalde gegevens worden met elkaar vergeleken. Indien een verschil wordt geconstateerd, dan wordt
    het betreffende gebied teruggegeven.
    """
    
    burggem = {
        1: '363',
        2: '344',
        3: '518',
        4: '599'
    }

    regio = {
        1: "('20','21','22')",
        2: "('23','24','25')",
        3: "('26','27','28','29')",
        4: "('30','31')"
    }

    wto = {
        2: "in ('T','H','K','V')",
        3: "= 'T'",
        4: "= 'H'",
        5: "= 'K'",
        6: "= 'V'",
        7: "= 'A'",
        8: "= 'O'"
    }
 

    # selecteer gegegevens uit PBK_INPUT
    # bij maandcontrole is alleen maxperiode gevuld
    gebiedfout = 'LN01'
    gebiedwhere = ''
    soortwhere = ''

    if kwartaal == '':
        periodewhere = f"""pbk_input.periodeid = '{maxperiode}' """
    else:
        # bij landelijke gegevens is het gebied niet gevuld
        if gebied == '':
            periodewhere = f"""pbk_input.periodeid = '{kwartaal}' """
            soortwhere = f"""and soortid = {soort} """
        else:
            periodewhere = f"""pbk_input.periodeid = '{kwartaal}' """
            # bij gegevens gegevens van grote steden is soort 9
            if soort == 9:
                gebiedfout = f"""GMO{burggem[gebied]}"""
                gebiedwhere = f"""and regioid = 'GMO{burggem[gebied]}' """
            elif soort == 10:
                # bij gegevens gegevens van provincies is soort 10
                gebiedfout = f"""PV{gebied}"""
                gebiedwhere = f"""and regioid = 'PV{gebied}' """
            elif soort == 11:
                # bij gegevens gegevens van provincies is soort 11
                gebiedfout = f"""LD0{gebied}'"""
                gebiedwhere = f"""and regioid = 'LD0{gebied}' """

    sql = f"""select  aantal_verkochte_woningen as aantal
                    , gemiddelde_verkoopprijs as gemkoopsom 
            from pbk_input 
            where {periodewhere} {gebiedwhere} {soortwhere}"""  

    df_pbkinput = pd.read_sql_query(sql, con=pg_client.get_bulk_connection())


    # selecteer gegevens uit de OBK_OV03_UITVOER
    # bij maandcontrole is alleen maxperiode gevuld
    fromwhere = 'where '
    gebiedwhere = ''
    soortwhere = ''

    if kwartaal == '':
        periodewhere = f"""periode = '{maxperiode}' """
    else:
        # bij landelijke gegevens is het gebied niet gevuld
        if gebied == '':
            # bij landelijk totaal is soort "1"
            periodewhere = f"""periode between '{minperiode}' and '{maxperiode}' """
            gebiedwhere = ''
            # bij landelijke gegevens naar woningtype is soort ongelijk "1"
            if soort != 1:
                soortwhere = f"""and wto {wto[soort]} """
        else:
            periodewhere = f"""periode between '{minperiode}' and '{maxperiode}' """
            # bij gegevens gegevens van grote steden is soort "9"
            if soort == 9:
                gebiedwhere = f"""and burggemnr = '{burggem[gebied]}' """
            elif soort == 10:
                # bij gegevens gegevens van provincies is soort "10"
                fromwhere = ', cbs_provincie_gemeente where '
                gebiedwhere = f"""and burggemnr = to_char(to_number(gemcode)) and provcode = '{gebied}' """
            elif soort == 11:
                # bij gegevens gegevens van regio's is soort "11"
                fromwhere = ', cbs_provincie_gemeente where '
                gebiedwhere = f"""and burggemnr = to_char(to_number(gemcode)) and provcode in {regio[gebied]} """

    sql = f"""select  count(1) as aantal
                    , round(avg(koopsom),0) as gemkoopsom 
              from ( select periode, koopsom 
                     from gma_own.obk_ov03_uitvoer 
                     {fromwhere} {periodewhere} {gebiedwhere} {soortwhere}
                    ) """

    df_ov03 = pd.read_sql_query(sql, con=ora_client.get_bulk_connection())


    # vergelijk de aantallen en gemiddelde koopsom tussen pbk_input en obk_ov03_uitvoer
    res = df_pbkinput.iloc[0].eq(df_ov03)
    if not res.aantal.bool() or not res.gemkoopsom.bool():
        fn_return_value = gebiedfout
    else:
        fn_return_value = ''

    return fn_return_value


if __name__ == "__main__":
     check_data()
