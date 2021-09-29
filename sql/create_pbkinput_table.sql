drop table if exists pbk.pbk_input;

create table pbk.pbk_input
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
