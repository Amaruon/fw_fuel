from DbConnector import PostgresApi
from tableClasses import WayBill, ExpandedRentedCar, MileageFuelEnd, FuelTransactions, RentedCar, TimeSheetRow
import calendar
from datetime import timedelta
import random as rd


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


class WayBillFiller:
    def __init__(self, pas, month: int, year: int, api=PostgresApi()):
        self.db = api
        self.password = pas
        self.db_array = []
        self.month = month
        self.year = year

    def get_data(self):
        self.db.connect_to_db(self.password)
        transactions = self.db.fetch_transactions(calendar.monthrange(self.year, self.month)[1], self.month)
        drivers = self.db.fetch_time_sheet(calendar.monthrange(self.year, self.month)[1], self.month)
        cars = self.db.fetch_rented_cars(calendar.monthrange(self.year, self.month)[1], self.month)

        return [transactions, drivers, cars]

    def set_transactions(self, transactions):
        for row in transactions:
            if isinstance(row, FuelTransactions):
                item = WayBill()
                item.card_number = row.card_number
                item.date = row.date
                item.entity = row.entity
                item.fuel = row.fuel
                item.fuel_get = row.liters

                self.db_array.append(item)

    @staticmethod
    def change_fuel_naming(row):
        if row.fuel.rstrip() == 'АИ-95':
            return 'Автомоб. бензин АИ-95'
        elif row.fuel.rstrip() == 'ДТ':
            return 'Дизельное топливо'

    @staticmethod
    def assign_driver(row, drivers):
        # exclude high refills
        if row.fuel == 'АИ-95' and row.fuel_get >= 60:
            return ['To tank', drivers]
        elif row.fuel == 'Дизельное топливо' and row.fuel_get >= 75:
            return ['To tank', drivers]
        else:
            if row.entity == 'Оп "Кольская"':
                row.entity = 'ОП "Каменск-Красносулинский"'
            for driver in drivers[:]:
                if isinstance(driver, TimeSheetRow) and driver.date == row.date \
                        and driver.entity == row.entity and row.card_number != '0':
                    drivers.remove(driver)
                    return [driver.acronym, drivers]
            return ['To tank', drivers]

    @staticmethod
    def expand_cars(cars):
        new_cars = []

        for car in cars:
            if isinstance(car, RentedCar):
                for date in date_range(car.rent_start, car.rent_end):
                    item = ExpandedRentedCar()
                    item.plate = car.plate
                    item.date = date
                    item.entity = car.entity
                    new_cars.append(item)
        return new_cars

    def assign_cars(self, row, cars):
        for car in cars[:]:
            if row.fuel == ''.join(self.db.fetch_fuel_type(car.plate)) and row.date == car.date\
                    and row.entity == car.entity:
                cars.remove(car)
                return [car.plate, cars]
            else:
                continue
        return ['No car', cars]

    def fuel_start(self, plate):    # work on it
        fuel_start = self.db.fetch_fuel_end(plate)
        return rd.uniform(40.000, 50.000) if fuel_start == 0 else fuel_start

    def mileage_start(self, plate):
        mileage_start = self.db.fetch_mileage_end(plate)
        return rd.uniform(11234.0, 45124.0) if mileage_start == 0 else mileage_start

    def calc_fuel(self, row):
        tank = self.db.fetch_tank_volume(row.car)
        fuel_start = self.fuel_start(row.car)
        fuel_get = row.fuel_get
        fuel_spent = rd.uniform(3.21, 7.76)

        if tank == 0:
            return None
        else:
            while True:
                if tank <= fuel_start + fuel_get - fuel_spent:
                    fuel_spent += 5.36
                else:
                    fuel_end = fuel_start + row.fuel_get - fuel_spent
                    return [fuel_start, fuel_spent, fuel_end]

    def calc_mileage(self, row):
        mileage_start = self.mileage_start(row.car)
        consumption = self.db.fetch_consumption(row.car)

        mileage_spent = 100 * row.fuel_spent / consumption
        mileage_end = mileage_start + mileage_spent

        return [mileage_start, mileage_spent, mileage_end]

    def fill(self):
        transactions, drivers, cars = self.get_data()
        self.set_transactions(transactions)
        cars = self.expand_cars(cars)

        for row in self.db_array:
            if row.card_number != '0':
                item = MileageFuelEnd()
                item.date = row.date
                row.fuel = self.change_fuel_naming(row)
                row.driver, drivers = self.assign_driver(row, drivers)
                row.car, cars = self.assign_cars(row, cars)
                if row.driver == 'To tank' or row.car == 'No car':
                    continue
                else:
                    item.plate = row.car
                    row.fuel_start, row.fuel_spent, item.fuel_end = self.calc_fuel(row)
                    row.mileage_start, row.mileage_spent, item.mileage_end = self.calc_mileage(row)
                    self.db.upload_to_db(item, self.password)
            else:
                continue

    def upload(self):
        self.db.upload_to_db(self.db_array, self.password)