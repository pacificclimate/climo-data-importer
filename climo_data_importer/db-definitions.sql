-- auto-generated definition
create table meta_network
(
    network_id           integer     default nextval('network_id_seq'::regclass) not null
        primary key,
    network_name         varchar(255),
    description          varchar(255),
    virtual              varchar(255),
    publish              boolean,
    col_hex              varchar(7),
    contact_id           integer
        references meta_contact
            on update cascade,
    mod_time             timestamp   default now()                               not null,
    mod_user             varchar(64) default CURRENT_USER                        not null,
    network_display_name varchar
        constraint uq_meta_network_network_display_name
            unique
);

create table meta_station
(
    station_id   integer     default nextval('station_id_seq'::regclass) not null
        primary key,
    network_id   integer
        references meta_network
            on update cascade,
    native_id    varchar(255),
    min_obs_time timestamp,
    max_obs_time timestamp,
    publish      boolean     default true                                not null,
    mod_time     timestamp   default now()                               not null,
    mod_user     varchar(64) default CURRENT_USER                        not null
);

create table meta_history
(
    history_id   integer     default nextval('history_id_seq'::regclass) not null
        primary key,
    station_id   integer
        constraint meta_history_station_id_fk
            references meta_station
            on update cascade,
    station_name varchar(255),
    lon          numeric,
    lat          numeric,
    elev         numeric,
    sdate        date,
    edate        date,
    tz_offset    interval,
    province     varchar(32),
    country      varchar(64),
    comments     varchar(255),
    the_geom     geometry(Geometry, 4326),
    sensor_id    integer
        constraint sensor_id
            references meta_sensor,
    freq         timescale,
    mod_time     timestamp   default now()                               not null,
    mod_user     varchar(64) default CURRENT_USER                        not null
);

create table meta_vars
(
    vars_id          integer     default nextval('vars_id_seq'::regclass) not null
        primary key,
    network_id       integer
        references meta_network
            on update cascade,
    unit             varchar(64)
        constraint ck_unit_no_newlines
            check ((unit)::text !~ '[
]'::text),
    precision        numeric,
    standard_name    varchar(64)                                          not null
        constraint ck_standard_name_no_newlines
            check ((standard_name)::text !~ '[
]'::text),
    cell_method      varchar(64)                                          not null
        constraint ck_cell_method_no_newlines
            check ((cell_method)::text !~ '[
]'::text),
    long_description varchar(256)
        constraint ck_long_description_no_newlines
            check ((long_description)::text !~ '[
]'::text),
    net_var_name     citext
        constraint ck_net_var_name_valid_identifier
            check (net_var_name ~ '^[a-zA-Z_][a-zA-Z0-9_$]*$'::citext),
    display_name     varchar(256)                                         not null
        constraint ck_display_name_no_newlines
            check ((display_name)::text !~ '[
]'::text),
    short_name       varchar(256)
        constraint ck_short_name_no_newlines
            check ((short_name)::text !~ '[
]'::text),
    mod_time         timestamp   default now()                            not null,
    mod_user         varchar(64) default CURRENT_USER                     not null,
    constraint network_variable_name_unique
        unique (network_id, net_var_name)
);

create table obs_raw
(
    obs_raw_id bigint      default nextval('obs_raw_id_seq'::regclass) not null
        primary key,
    obs_time   timestamp,
    mod_time   timestamp   default now()                               not null,
    datum      real,
    vars_id    integer                                                 not null
        references meta_vars,
    history_id integer
        constraint hist_id_fk
            references meta_history,
    mod_user   varchar(64) default CURRENT_USER                        not null,
    constraint time_place_variable_unique
        unique (history_id, vars_id, obs_time)
);