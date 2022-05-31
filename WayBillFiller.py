from DbConnector import PostgresApi
from tableClasses import WayBill, TimeSheetRow
import calendar


class WayBillFiller:
    def __init__(self, pas, month: int, year: int, api=PostgresApi()):
        self.db = api
        self.password = pas
        self.db_array = None
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

    def change_fuel_naming(self, row):
        if row.fuel.rstrip() == 'АИ-95':
            return 'Автомоб. бензин АИ-95'
        elif row.fuel.rstrip() == 'ДТ':
            return 'Дизельное топливо'

    def assign_driver(self, row, drivers):
        # exclude high refills
        if row.fuel == 'АИ-95' and row.fuel_get >= 60:
            return 'To tank'
        elif row.fuel == 'Дизельное топливо' and row.fuel_get >= 75:
            return 'To tank'
        else:
            for driver in drivers[:]:
                if driver.date == row.date and driver.entity == row.entity and row.card_number != '0':
                    row.driver = driver.driver
                    drivers.remove(driver)
                    return driver.driver, drivers

    def calc(self):
        transactions, drivers, cars = self.get_data()
        self.set_transactions(transactions)

        for row in self.db_array:
            row.fuel = self.change_fuel_naming(row)
            row.driver, drivers = self.assign_driver(row, drivers)


