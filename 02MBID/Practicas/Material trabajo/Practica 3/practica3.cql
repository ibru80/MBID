CREATE KEYSPACE hospitals
WITH replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};
CREATE TABLE hospitals.countypk (
county text,todays_date date,all_hospital_beds int,hospitalized_covid_confirmed_patients int,hospitalized_covid_patients int,
hospitalized_suspected_covid_patients int,icu_available_beds int,icu_covid_confirmed_patients int,icu_suspected_covid_patients int,
PRIMARY KEY (county, todays_date));

CREATE TABLE hospitals.datepk (
	county text,
	todays_date date,
	all_hospital_beds int,
	hospitalized_covid_confirmed_patients int,
	hospitalized_covid_patients int,
	hospitalized_suspected_covid_patients int,
	icu_available_beds int,
	icu_covid_confirmed_patients int,
	icu_suspected_covid_patients int,
	PRIMARY KEY (todays_date, county)
) WITH bloom_filter_fp_chance = 0.01
AND comment = ''
AND crc_check_chance = 1.0
AND dclocal_read_repair_chance = 0.1
AND default_time_to_live = 0
AND gc_grace_seconds = 864000
AND max_index_interval = 2048
AND memtable_flush_period_in_ms = 0
AND min_index_interval = 128
AND read_repair_chance = 0.0
AND speculative_retry = '99.0PERCENTILE'
AND caching = {
	'keys' : 'ALL',
	'rows_per_partition' : 'NONE'
}
AND compression = {
	'chunk_length_in_kb' : 64,
	'class' : 'LZ4Compressor',
	'enabled' : true
}
AND compaction = {
	'class' : 'SizeTieredCompactionStrategy',
	'max_threshold' : 32,
	'min_threshold' : 4
};
