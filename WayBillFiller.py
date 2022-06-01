from DbConnector import PostgresApi
from tableClasses import WayBill, ExpandedRentedCar, MileageFuelEnd
from func import date_range
import calendar
import random as rd


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

        return transactions, drivers, cars

    def set_transactions(self, transactions):
        for row in transactions:
            item = WayBill()
            item.card_number, item.date, item.entity, \
            item.fuel, item.fuel_get = row

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
            return 'To tank'
        elif row.fuel == 'Дизельное топливо' and row.fuel_get >= 75:
            return 'To tank'
        else:
            for driver in drivers[:]:
                if driver.date == row.date and driver.entity == row.entity and row.card_number != '0':
                    drivers.remove(driver)
                    return driver.driver, drivers

    @staticmethod
    def expand_cars(cars):
        new_cars = []

        for car in cars:
            for date in date_range(car.rent_start, car.rent_end):
                item = ExpandedRentedCar()
                item.plate = car.plate
                item.date = date
                new_cars.append(item)
        return new_cars

    def assign_cars(self, row, cars):
        for car in cars[:]:
            if row.fuel == self.db.fetch_fuel_type(car.plate).rstrip() and row.date == car.date:
                cars.remove(car)
                return car.plate, cars

    def fuel_start(self, plate):    # work on it
        fuel_start = self.db.fetch_fuel_end(plate)
        return rd.uniform(40.000, 50.000) if fuel_start == 0 else fuel_start

    def mileage_start(self, plate):
        mileage_start = self.db.fetch_mileage_end(plate)
        return rd.uniform(11234.0, 45124.0) if mileage_start == 0 else mileage_start

    def calc_fuel(self, row):
        tank = self.db.fetch_tank_volume(row.plate)
        fuel_start = self.fuel_start(row.plate)
        fuel_get = row.fuel_get
        fuel_spent = rd.uniform(4.21, 5.76)

        if tank == 0:
            return None
        else:
            while True:
                if tank <= fuel_start + fuel_get - fuel_spent:
                    diff = tank - fuel_start - fuel_get + fuel_spent
                    fuel_spent += rd.uniform(diff + 2.34, diff - 2.37)
                else:
                    fuel_end = fuel_start + row.fuel_get - fuel_spent
                    return fuel_start, fuel_spent, fuel_end

    def calc_mileage(self, row):
        mileage_start = self.mileage_start(row.plate)
        consumption = self.db.fetch_consumption(row.plate)

        mileage_spent = consumption * row.fuel_spent
        mileage_end = mileage_start + mileage_spent

        return mileage_start, mileage_spent, mileage_end

    def fill(self):
        transactions, drivers, cars = self.get_data()
        self.set_transactions(transactions)
        cars = self.expand_cars(cars)

        for row in self.db_array:
            item = MileageFuelEnd()
            item.plate = row.date
            row.fuel = self.change_fuel_naming(row)
            row.driver, drivers = self.assign_driver(row, drivers)
            row.plate, cars = self.assign_cars(row, cars)
            item.plate = row.plate
            row.fuel_start, row.fuel_spent, item.fuel_end = self.calc_fuel(row)
            row.mileage_start, row.mileage_spent, item.mileage_end = self.calc_mileage(row)
            self.db.upload_to_db(item, self.password)
