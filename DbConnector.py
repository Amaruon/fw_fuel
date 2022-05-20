import psycopg2
import datetime
import pandas as pd
from tableClasses import Worker, RentedCar, Car, FuelTransactions, TimeSheetRow, MileageFuelEnd


class Sql:
    def __init__(self):
        self._conn = None
        self.__cursor = None

    @property
    def cursor(self): return self.__cursor

    @cursor.setter
    def cursor(self, value):
        self.__cursor = value

    def connect(self, password):
        try:
            self._conn = psycopg2.connect(
                dbname='fuel_db',
                user='postgres',
                password=password,
                host='localhost',
                port=5432
            )
            print("Connection was successful")
            self.__cursor = self._conn.cursor()
        except Exception as Err:
            print(f'Error while connecting: {Err}')

    def query(self, query):
        self._conn.autocommit = True
        try:
            self.cursor.execute(query)
            print('Query was completed')
        except Exception as Err:
            print(f'Error in query: "{Err}"')

    def fetch_query(self, query):
        try:
            self.query(query)
            return pd.DataFrame(self.cursor.fetchall())
        except Exception as err:
            print(f'Error: {err}')

    def close_connection(self):
        self._conn.close()
        self.__cursor.close()


class PostgresApi:
    def __init__(self):
        self.db = Sql()

    def connect_to_db(self, pwd):
        self.db.connect(pwd)

    def exit(self):
        self.db.close_connection()

    def upload_to_db(self, array, pwd):
        self.connect_to_db(pwd)

        for row in array:
            self.upload_queries(row)

        self.exit()

    def upload_queries(self, value):
        try:
            if isinstance(value, Worker):
                self.db.query(f'''
                    INSERT INTO worker (acronym, ru_name, eng_name, driver_licence)
                    VALUES ('{value.acronym}', '{value.ru_name}', '{value.eng_name}', '{value.driver_license}');''')

            elif isinstance(value, RentedCar):
                self.db.query(f'''
                    INSERT INTO rented_car (plate, rent_start, rent_end) 
                    VALUES ('{value.plate}', '{value.rent_start}', '{value.rent_end}')
                ''')

            elif isinstance(value, Car):
                self.db.query(f'''
                    INSERT INTO car (plate, model, vin, owner, fuel, is_special, tank_volume, consumption_rate) 
                    VALUES ('{value.plate}', '{value.model}', '{value.vin}', '{value.owner}', '{value.fuel}', 
                            '{value.is_special}', '{value.tank_volume}', '{value.consumption_rate}') 
                ''')

            elif isinstance(value, FuelTransactions):
                self.db.query(f'''
                    INSERT INTO fuel_transactions (card_number, date, entity, fuel, liters, price, total_price)
                    VALUES (
                        '{value.card_number}', '{value.date}', '{value.entity}', 
                        '{value.fuel}', '{value.liters}', '{value.price}', '{value.total_price}'
                            )
                ''')

            elif isinstance(value, TimeSheetRow):
                self.db.query(f'''
                    INSERT INTO time_sheet (date, acronym, type_of_time, hours_worked, entity)
                    VALUES (
                    '{value.date}', '{value.acronym}', '{value.type_of_time}', '{int(value.hours_worked)}', '{value.entity}'
                    )
                ''')

            elif isinstance(value, MileageFuelEnd):
                self.db.query(f'''
                    INSERT INTO mileage_fuel_end (plate, date, mileage_end, fuel_end)
                    VALUES (
                    '{value.plate}', '{value.date}', '{value.mileage_end}', '{value.fuel_end}'
                    )
                ''')

        except Exception as err:
            print(f'Error: {err}')

    def fetch_transactions(self, entity, f_day, l_day, month):

        df = self.db.fetch_query(f'''
            SELECT card_number, date, entity, fuel, liters FROM fuel_transactions
            WHERE date >= '{datetime.date(2022, month, f_day)}' and 
            date <= '{datetime.date(2022, month, l_day)}' and
            entity = '{entity}'
            ORDER BY card_number, date 
        ''')

        df.columns = ['card_number', 'date', 'entity', 'fuel', 'liters']
        return df

    def fetch_time_sheet(self, entity, f_day, l_day, month):

        df = self.db.fetch_query(f'''
            SELECT date, acronym, type_of_time, hours_worked, entity FROM time_sheet
            WHERE entity = '{entity}' and 
            date >= '{datetime.date(2022, month, f_day)}' and 
            date <= '{datetime.date(2022, month, l_day)}' and
            entity = '{entity}'
            ORDER BY acronym
        ''')

        df.columns = ['date', 'acronym', 'type_of_time', 'hours_worked', 'entity']

        return df

    def fetch_rented_cars(self, f_day, l_day, month):

        df = self.db.fetch_query(f'''
            SELECT plate, rent_start, rent_end FROM rented_car
            WHERE rent_end >= '{datetime.date(2022, month, f_day)}' and 
            rent_end <= '{datetime.date(2022, month, l_day)}'
            ORDER BY rent_start
        ''')

        df.columns = ['plate', 'rent_start', 'rent_end']

        return df

    def fetch_fuel_type(self, plate):
        fuel_type = self.db.fetch_query(f'''
            SELECT fuel FROM car
            WHERE plate = '{plate}'
        ''')

        return fuel_type.iat[0, 0]

    def fetch_fuel_end(self, plate):
        fuel_end = self.db.fetch_query(f'''
            SELECT DISTINCT ON (plate)
            fuel_end
            FROM mileage_fuel_end
            WHERE plate = '{plate}'
            ORDER BY plate, date DESC
        ''')

        return fuel_end.iat[0, 0]

    def fetch_mileage_end(self, plate):
        mileage_end = self.db.fetch_query(f'''
            SELECT DISTINCT ON (plate)
            mileage_end
            FROM mileage_fuel_end
            WHERE plate = '{plate}'
            ORDER BY plate, date DESC
        ''')

        return mileage_end.iat[0, 0]

    def fetch_tank_volume(self, plate):
        tank_volume = self.db.fetch_query(f'''
            SELECT tank_volume FROM car
            WHERE plate = '{plate}'
        ''')

        return tank_volume.iat[0, 0]

    def fetch_consumption(self, plate):
        consumption = self.db.fetch_query(f'''
            SELECT consumption_rate FROM car
            WHERE plate = '{plate}'
        ''')

        return consumption.iat[0, 0]
