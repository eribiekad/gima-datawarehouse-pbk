create table indices
(
    index                             bigint,
    periodeid                         text,
    periode                           text,
    regioid                           text,
    regio                             text,
    soortid                           bigint,
    soort                             text,
    index_2015                        double precision,
    mutatie_vorige_periode            double precision,
    mutatie_zelfde_periode_vorig_jaar double precision,
    aantal_verkochte_woningen         bigint,
    gemiddelde_verkoopprijs           bigint
);

create unique index indices_periodeid_regioid_soortid_uk
    on indices (periodeid, regioid, soortid);