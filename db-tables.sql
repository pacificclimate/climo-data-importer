-- auto-generated definition
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

create index mod_time_idx
    on obs_raw (mod_time);

create index obs_raw_comp_idx
    on obs_raw (obs_raw_id, obs_time, vars_id, history_id);

create index obs_raw_history_id_idx
    on obs_raw (history_id);

create index obs_raw_id_idx
    on obs_raw (obs_raw_id);

create index ix_crmp_obs_raw_vars_id
    on obs_raw (vars_id);

create index ix_crmp_obs_raw_mod_user
    on obs_raw (mod_user);

-- auto-generated definition
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

create index fki_meta_history_station_id_fk
    on meta_history (station_id);

create index meta_history_freq_idx
    on meta_history (freq);

    -- auto-generated definition
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

create index fki_meta_station_network_id_fkey
    on meta_station (network_id);
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