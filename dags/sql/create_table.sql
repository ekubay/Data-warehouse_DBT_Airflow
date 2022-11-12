CREATE TABLE IF NOT EXISTS trajectory
(
    id integer serial,
    track_id text default NOT NULL,
    type character varying(400) default NOT NULL,
    traveled_dis double precision NOT NULL,
    avg_speed double precision,
    longuited text NOT NULL,
    latitude text default NOT NULL,
    speed double precision NOT NULL,
    lon_acc text default NULL,
    lat_acc text COLLATE default NOT NULL,
    time double precision,
    CONSTRAINT trajectory_pkey PRIMARY KEY (id)
); 

ALTER TABLE IF EXISTS public.trajectory
    OWNER to postgres;