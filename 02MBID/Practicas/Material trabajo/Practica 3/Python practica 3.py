# This is a sample Python script.
import csv
import time
from cassandra.cluster import Cluster

import datetime
class hospitalInfo:
    def __init__(self, county,todays_date,hospitalized_covid_confirmed_patients,hospitalized_suspected_covid_patients,hospitalized_covid_patients,all_hospital_beds,icu_covid_confirmed_patients,icu_suspected_covid_patients,icu_available_beds):
        self.county = county
        self.todays_date = todays_date
        if (not hospitalized_covid_confirmed_patients == ''):
            self.hospitalized_covid_confirmed_patients = int(float(hospitalized_covid_confirmed_patients))
        else:
            self.hospitalized_covid_confirmed_patients = 0
        if (not hospitalized_suspected_covid_patients == ''):
            self.hospitalized_suspected_covid_patients =int(float(hospitalized_suspected_covid_patients))
        else:
            self.hospitalized_suspected_covid_patients = 0
        if (not hospitalized_covid_patients == ''):
            self.hospitalized_covid_patients =int(float(hospitalized_covid_patients))
        else:
            self.hospitalized_covid_patients = 0
        if (not all_hospital_beds == ''):
            self.all_hospital_beds =int(float(all_hospital_beds))
        else:
            self.all_hospital_beds = 0
        if (not icu_covid_confirmed_patients == ''):
            self.icu_covid_confirmed_patients =int(float(icu_covid_confirmed_patients))
        else:
            self.icu_covid_confirmed_patients = 0
        if (not icu_suspected_covid_patients == ''):
            self.icu_suspected_covid_patients =int(float(icu_suspected_covid_patients))
        else:
            self.icu_suspected_covid_patients = 0
        if (not icu_available_beds == ''):
            self.icu_available_beds =int(float(icu_available_beds))
        else:
            self.icu_available_beds = 0
def insertData ():
    with open ('covid19hospitalbycounty.csv', newline='') as csvhospitals:
        spamreader = csv.DictReader(csvhospitals,)
        insertStatement = session.prepare(
            "INSERT INTO countypk (county,todays_date,hospitalized_covid_confirmed_patients,hospitalized_suspected_covid_patients,"
            "hospitalized_covid_patients,all_hospital_beds,icu_covid_confirmed_patients,icu_suspected_covid_patients,icu_available_beds) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        futures = []
        for row in spamreader:
            dataCovid = hospitalInfo(row['county'], row['todays_date'], row['hospitalized_covid_confirmed_patients'], row['hospitalized_suspected_covid_patients'],
                                     row['hospitalized_covid_patients'], row['all_hospital_beds'], row['icu_covid_confirmed_patients'], row['icu_suspected_covid_patients'], row['icu_available_beds'])

            futures.append(session.execute_async(insertStatement, [dataCovid.county,dataCovid.todays_date,
                                                                   dataCovid.hospitalized_covid_confirmed_patients,
                                                                   dataCovid.hospitalized_suspected_covid_patients,
                                                                   dataCovid.hospitalized_covid_patients,dataCovid.all_hospital_beds,
                                                                   dataCovid.icu_covid_confirmed_patients,dataCovid.icu_suspected_covid_patients,
                                                                   dataCovid.icu_available_beds]))
        # Garantiza que el programa no finalice hasta que se ejecuten todas las inserciones
        for f in futures:
            f.result()  # bloquea hasta que se ejecuta la inserción
def insertDataTableDatePk ():
    with open ('covid19hospitalbycounty.csv', newline='') as csvhospitals:
        spamreader = csv.DictReader(csvhospitals,)
        insertStatement = session.prepare(
            "INSERT INTO datepk (county,todays_date,hospitalized_covid_confirmed_patients,hospitalized_suspected_covid_patients,hospitalized_covid_patients,all_hospital_beds,icu_covid_confirmed_patients,icu_suspected_covid_patients,icu_available_beds) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        futures = []
        for row in spamreader:
            dataCovid = hospitalInfo(row['county'], row['todays_date'], row['hospitalized_covid_confirmed_patients'], row['hospitalized_suspected_covid_patients'],
                                     row['hospitalized_covid_patients'], row['all_hospital_beds'], row['icu_covid_confirmed_patients'], row['icu_suspected_covid_patients'], row['icu_available_beds'])

            futures.append(session.execute_async(insertStatement, [dataCovid.county,dataCovid.todays_date,dataCovid.hospitalized_covid_confirmed_patients,dataCovid.hospitalized_suspected_covid_patients,dataCovid.hospitalized_covid_patients,dataCovid.all_hospital_beds,dataCovid.icu_covid_confirmed_patients,dataCovid.icu_suspected_covid_patients,dataCovid.icu_available_beds]))
        # Catch any remaining async requests that haven't finished
        for f in futures:
            f.result()  # blocks until remaining inserts are completed.
cluster = Cluster()
session = cluster.connect('hospitals')
tic = time.perf_counter()
insertData()
toc = time.perf_counter()
print(f"Tiempo de inyección de datos {toc - tic:0.4f} seconds")
tic = time.perf_counter()
insertDataTableDatePk()
toc = time.perf_counter()
print(f"Tiempo de inyección de datos {toc - tic:0.4f} seconds")



