import psycopg2
import datetime
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
            return [self.cursor.fetchall()]
        except Exception as err:
            print(f'Error: {err}')

    def close_connection(self):
        self._conn.close()
        self.__cursor.close()


class PostgresApi:
    def __init__(self):
        self.db = Sql()
        self.class_list = []

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
                    INSERT INTO worker (acronym, ru_name, eng_name, driver_license)
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
                    INSERT INTO fuel_card (card_number, date, entity, fuel, liters, price, total_price)
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

    def fetch_transactions(self, l_day, month):

        array = self.db.fetch_query(f'''
            SELECT card_number, date, entity, fuel, liters FROM fuel_transactions
            WHERE date >= '{datetime.date(2022, month, 1)}' and 
            date <= '{datetime.date(2022, month, l_day)}' and
            ORDER BY entity, card_number, date 
        ''')
        for row in array:
            item = FuelTransactions()
            item.card_number, item.date, item.entity, item.fuel, item.liters = row
            self.class_list.append(item)

        return self.class_list

    def fetch_time_sheet(self, l_day, month):

        array = self.db.fetch_query(f'''
            SELECT date, acronym, type_of_time, hours_worked, entity FROM time_sheet
            date >= '{datetime.date(2022, month, 1)}' and 
            date <= '{datetime.date(2022, month, l_day)}' and
            ORDER BY acronym
        ''')

        for row in array:
            item = TimeSheetRow()
            item.date, item.acronym, item.type_of_time, item.hours_worked, item.entity = row
            self.class_list.append(item)

        return self.class_list

    def fetch_rented_cars(self, l_day, month):

        array = self.db.fetch_query(f'''
            SELECT plate, rent_start, rent_end FROM rented_car
            WHERE rent_start >= '{datetime.date(2022, month, 1)}' and 
            rent_end <= '{datetime.date(2022, month, l_day)}'
            ORDER BY rent_start
        ''')

        for row in array:
            item = RentedCar()
            item.plate, item.rent_start, item.rent_end = row
            self.class_list.append(item)

        return self.class_list

    def fetch_fuel_type(self, plate):
        fuel_type = self.db.fetch_query(f'''
            SELECT fuel FROM car
            WHERE plate = '{plate}'
        ''')

        return fuel_type[0]

    def fetch_fuel_end(self, plate):
        fuel_end = self.db.fetch_query(f'''
            SELECT DISTINCT ON (plate)
            fuel_end
            FROM mileage_fuel_end
            WHERE plate = '{plate}'
            ORDER BY plate, date DESC
        ''')
        while True:
            try:
                return float(fuel_end[0])
            except IndexError:
                return 0

    def fetch_mileage_end(self, plate):
        mileage_end = self.db.fetch_query(f'''
            SELECT DISTINCT ON (plate)
            mileage_end
            FROM mileage_fuel_end
            WHERE plate = '{plate}'
            ORDER BY plate, date DESC
        ''')
        while True:
            try:
                return float(mileage_end[0])
            except IndexError:
                return 0

    def fetch_tank_volume(self, plate):
        tank_volume = self.db.fetch_query(f'''
                        SELECT tank_volume FROM car
                        WHERE plate = '{plate}'
                    ''')

        while True:
            try:
                return tank_volume[0]
            except IndexError:
                return 0

    def fetch_consumption(self, plate):
        consumption = self.db.fetch_query(f'''
            SELECT consumption_rate FROM car
            WHERE plate = '{plate}'
        ''')

        return consumption[0]
