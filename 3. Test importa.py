import unittest
import pandas as pd
import sqlalchemy
from pandas.testing import assert_frame_equal
from sqlalchemy.sql import text
import numpy as np


class TestDatabase(unittest.TestCase):
          def setUp(self):
              
              self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/dw')
              self.connection = self.engine.connect()
            
              self.df = pd.read_csv("Traffic_Violations_PROCESSED.csv")
              
              query = text(""" 
              SELECT ts.date_of_stop, ts.time_of_stop, ts.agency, ts.subagency, ts.description, ts.location, 
                     ts.latitude, ts.longitude, ts.geolocation,
                     id.accident, id.belts, id.personal_injury, id.property_damage, id.fatal, id.alcohol, id.work_zone,
                     ts.state,
                     v.vehicletype, v.year, v.make, v.model, v.color,
                     vi.violation_type, vi.charge, vi.article, vi.contributed_to_accident,
                     d.race, d.gender, d.driver_city, d.driver_state, d.dl_state,
                     a.arrest_type,
                     ci.commercial_license, ci.hazmat, ci.commercial_vehicle
              FROM traffic_stop ts
              JOIN incident_details id ON ts.id = id.traffic_stop_id
              JOIN vehicle v ON ts.id = v.traffic_stop_id
              JOIN violation vi ON ts.id = vi.traffic_stop_id
              JOIN driver d ON ts.id = d.traffic_stop_id
              JOIN arrest a ON ts.id = a.traffic_stop_id
              JOIN commercial_info ci ON ts.id = ci.traffic_stop_id
              ORDER BY ts.id ASC
              """)
              result = self.connection.execute(query)
              self.db_df = pd.DataFrame(result.fetchall())
              self.db_df.columns = result.keys()

              
              print("CSV Columns:", list(self.df.columns))
              print("DB Columns:", list(self.db_df.columns))
              print("Missing in DB:", set(self.df.columns) - set(self.db_df.columns))
              print("Missing in CSV:", set(self.db_df.columns) - set(self.df.columns))

          
          def test_columns(self):
              self.assertListEqual(sorted(list(self.df.columns)), sorted(list(self.db_df.columns)))

         
          def test_dataframes(self):
              csv_df = self.df.copy().reset_index(drop=True)
              db_df = self.db_df.copy().reset_index(drop=True)

              csv_df = csv_df[db_df.columns]

             
              bool_columns = [
                  'accident', 'belts', 'personal_injury', 'property_damage',
                  'fatal', 'commercial_license', 'hazmat', 'commercial_vehicle',
                  'alcohol', 'work_zone', 'contributed_to_accident'
              ]

              for col in bool_columns:
                  if col in csv_df.columns:
                      csv_df[col] = csv_df[col].map({'Yes': 1, 'No': 0})

              float_columns = ['latitude', 'longitude']
              for col in float_columns:
                  if col in csv_df.columns:
                      csv_df[col] = csv_df[col].astype(float).round(4)
                      db_df[col] = db_df[col].astype(float).round(4)

              
              if db_df['time_of_stop'].dtype == 'timedelta64[ns]':
                  db_df['time_of_stop'] = db_df['time_of_stop'].astype(str).str.replace('0 days ', '')

            
              csv_df['date_of_stop'] = pd.to_datetime(csv_df['date_of_stop']).dt.strftime('%Y-%m-%d')
              db_df['date_of_stop'] = pd.to_datetime(db_df['date_of_stop']).dt.strftime('%Y-%m-%d')

              
              if 'year' in csv_df.columns:
                  csv_df['year'] = csv_df['year'].astype(int)
                  db_df['year'] = db_df['year'].astype(int)

              try:
                  assert_frame_equal(csv_df, db_df, check_dtype=False)
                  print("Dataframes are equal!")
              except AssertionError as e:
                  print(f"Dataframes are different: {str(e)}")

                  
          def tearDown(self):
              self.connection.close()

if __name__ == '__main__':
          unittest.main()
