insert into pbk.indices
                                (   periodeid,
                                    periode,
                                    regioid,
                                    regio,
                                    soortid,
                                    soort,
                                    index_2015,
                                    mutatie_vorige_periode,
                                    mutatie_zelfde_periode_vorig_jaar,
                                    aantal_verkochte_woningen,
                                    gemiddelde_verkoopprijs
                                 )
    select  periodeid,
            periode,
            regioid,
            regio,
            soortid,
            soort,
            index_2015,
            mutatie_vorige_periode,
            mutatie_zelfde_periode_vorig_jaar,
            aantal_verkochte_woningen,
            gemiddelde_verkoopprijs from pbk.pbk_input
        ON CONFLICT (periodeid,regioid,soortid)
    DO UPDATE
    SET
        periodeid = EXCLUDED.periodeid,
        periode = EXCLUDED.periodeid,
        regioid = EXCLUDED.regioid,
        regio = EXCLUDED.regio,
        soortid = EXCLUDED.soortid,
        soort = EXCLUDED.soort,
        index_2015 = EXCLUDED.index_2015,
        mutatie_vorige_periode = EXCLUDED.mutatie_vorige_periode,
        mutatie_zelfde_periode_vorig_jaar = EXCLUDED.mutatie_zelfde_periode_vorig_jaar,
        aantal_verkochte_woningen = EXCLUDED.aantal_verkochte_woningen,
        gemiddelde_verkoopprijs = EXCLUDED.gemiddelde_verkoopprijs