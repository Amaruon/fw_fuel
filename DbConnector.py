import psycopg2
import datetime
from tableClasses import Worker, RentedCar, Car, FuelTransactions, TimeSheetRow, MileageFuelEnd, WayBill
from _collections_abc import Iterable


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
        except Exception as Err:
            print(f'Error: "{Err}\nin query: {query}"')

    def fetch_query(self, query):
        try:
            self.query(query)
            return self.cursor.fetchall()
        except Exception as err:
            print(f'Error: {err}')

    def fetch_numeric_query(self, query):
        try:
            self.query(query)
            return self.__cursor.fetchone()
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
        self.db.connect(pwd)
        if isinstance(array, Iterable):
            for row in array:
                self.upload_queries(row)
        else:
            self.upload_queries(array)

    def upload_queries(self, value):
        try:
            if isinstance(value, Worker):
                self.db.query(f'''
                    INSERT INTO worker (acronym, ru_name, eng_name, driver_license)
                    VALUES ('{value.acronym}', '{value.ru_name}', '{value.eng_name}', '{value.driver_license}');''')

            elif isinstance(value, RentedCar):
                self.db.query(f'''
                    INSERT INTO rented_car (plate, rent_start, rent_end, entity) 
                    VALUES ('{value.plate}', '{value.rent_start}', '{value.rent_end}', '{value.entity}')
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
                    INSERT INTO time_sheet_row (date, acronym, type_of_time, hours_worked, entity)
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

            elif isinstance(value, WayBill):
                self.db.query(f'''
                    INSERT INTO way_bill (card_number, date, driver, car, 
                                          fuel_start, fuel_spent, mileage_start, mileage_spent)
                    VALUES (
                        '{value.card_number}', '{value.date}', '{value.driver}', '{value.car}', '{value.fuel_start}',
                        '{value.fuel_spent}', '{value.mileage_start}', '{value.mileage_spent}'
                    )
                ''')

        except Exception as err:
            print(f'Error: {err}')

    def fetch_transactions(self, l_day, month):

        array = self.db.fetch_query(f'''
            SELECT card_number, date, entity, fuel, liters FROM fuel_card
            WHERE date >= '{datetime.date(2022, month, 1)}' and 
            date <= '{datetime.date(2022, month, l_day)}'
            ORDER BY date, card_number
        ''')
        for row in array:
            item = FuelTransactions()
            item.card_number, item.date, item.entity, item.fuel, item.liters = row
            self.class_list.append(item)

        return self.class_list

    def fetch_time_sheet(self, l_day, month):

        array = self.db.fetch_query(f'''
            SELECT tsr.date, tsr.acronym, tsr.type_of_time, tsr.hours_worked, tsr.entity FROM time_sheet_row tsr, worker w
            WHERE tsr.date >= '{datetime.date(2022, month, 1)}' and 
            tsr.date <= '{datetime.date(2022, month, l_day)}' and
            tsr.acronym = w.acronym and
            w.driver_license = 'True'
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
        print(plate)
        while True:
            try:
                fuel_end = str(fuel_end[0])
                fuel_end = fuel_end.replace('(', '')
                fuel_end = fuel_end.replace(',)', '')
                return float(fuel_end)
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
                mileage_end = str(mileage_end[0])
                mileage_end = mileage_end.replace('(', '')
                mileage_end = mileage_end.replace(',)', '')
                return float(mileage_end)
            except IndexError:
                return 0

    def fetch_tank_volume(self, plate):
        tank_volume = self.db.fetch_numeric_query(f'''
                        SELECT tank_volume FROM car
                        WHERE plate = '{plate}'
                    ''')
        while True:
            try:
                tank_volume = str(tank_volume)
                tank_volume = tank_volume.replace('(', '')
                tank_volume = tank_volume.replace(',)', '')
                return int(tank_volume)
            except IndexError:
                return 0

    def fetch_consumption(self, plate):
        consumption = self.db.fetch_numeric_query(f'''
            SELECT consumption_rate FROM car
            WHERE plate = '{plate}'
        ''')
        consumption = str(consumption)
        consumption = consumption.replace('(', '')
        consumption = consumption.replace(',)', '')
        consumption = float(consumption)
        return consumption

    def fetch_result(self):
        result = self.db.fetch_query(f'''
            SELECT wb.*, fc.liters, mfe.fuel_end, mfe.mileage_end, 
            tsr.entity, w.ru_name, w.driver_license, c.consumption_rate from way_bill wb
            LEFT JOIN fuel_card fc on wb.card_number = fc.card_number and wb.date = fc.date
            left join mileage_fuel_end mfe on mfe.date = wb.date and mfe.plate = wb.car
            left join time_sheet_row tsr on tsr.date = wb.date and tsr.acronym = wb.driver
            left join worker w on w.acronym = wb.driver
            left join car c on c.plate = wb.car;
        ''')

        return result
