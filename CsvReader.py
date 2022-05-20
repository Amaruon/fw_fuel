import pandas as pd
import re
from datetime import datetime
from tableClasses import Worker, RentedCar, Car, TimeSheetRow, FuelTransactions, MileageFuelEnd


class CsvReader:
    def __init__(self):
        self.file = None
        self.array = None

    def open_csv(self, file):
        try:
            self.array = pd.read_csv(file).to_numpy()
        except Exception as err:
            print(f'Error: {err}')

    def rent_dates(self, file):
        self.open_csv(file)
        rented_cars = []
        for row in self.array:
            plate = r'[А-ЯA-Z][0-9]{3}[А-ЯA-Z]{2}[0-9]{3}'
            dates = r'\d\d/\d\d/\d\d - \d\d/\d\d/\d\d'
            if re.search(plate, row[0]) and re.search(dates, row[0]):
                dates_list = re.findall(r'\d\d/\d\d/\d\d', row[0])
                item = RentedCar()
                item.plate = re.findall(plate, row[0])[0]
                item.rent_start = datetime.strptime(dates_list[0], '%d/%m/%y').date()
                item.rent_end = datetime.strptime(dates_list[1], '%d/%m/%y').date()
                rented_cars.append(item)
                print(item.plate)
            else:
                pass
        return rented_cars

    def workers(self, file):
        self.open_csv(file)
        workers = []
        for row in self.array:
            item = Worker()
            item.acronym = row[0]
            item.ru_name = row[1]
            item.eng_name = row[2]
            item.driver_license = bool(row[3])
            workers.append(item)
        return workers

    def cars(self, file):
        self.open_csv(file)
        cars = []
        for row in self.array:
            plate = r'[А-ЯA-Z][0-9]{3}[А-ЯA-Z]{2}[0-9]{3}'
            if re.search(plate, row[0]):
                item = Car()
                item.plate = row[0]
                item.model = row[1]
                item.vin = row[2]
                item.owner = row[3]
                item.fuel = row[4]
                item.is_special = True if row[5] == 'Да' else False
                item.tank_volume = row[6]
                item.consumption_rate = row[7]
                cars.append(item)
        return cars

    def time_sheet(self, file):
        self.open_csv(file)
        rows = []

        for row in self.array:
            item = TimeSheetRow()
            item.date = datetime.strptime(row[0], '%d.%m.%Y').date()
            item.acronym = row[1]
            item.type_of_time = row[2]
            item.hours_worked = row[3]
            item.entity = row[4]
            rows.append(item)

        return rows

    def fuel_transactions(self, file):
        self.open_csv(file)
        transactions = []

        for row in self.array:
            item = FuelTransactions()
            item.card_number = row[0]
            item.date = row[1]
            item.entity = row[2]
            item.fuel = row[3]
            item.liters = row[4]
            item.price = row[5]
            item.total_price = row[6]

            transactions.append(item)

        return transactions

    def mileage_fuel_end(self, file):
        self.open_csv(file)
        end_data = []

        for row in self.array:
            item = MileageFuelEnd()
            item.plate = row[0]
            item.date = datetime.strptime(row[1], '%d.%m.%Y').date()
            item.mileage_end = row[2]
            item.fuel_end = row[3]

            end_data.append(item)

        return end_data
