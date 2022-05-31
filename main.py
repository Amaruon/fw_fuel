from DbConnector import PostgresApi
from tableClasses import MileageFuelEnd
from datetime import timedelta
import pandas as pd
import random as rd


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n + 1)


def calculate_fuel_consumption():
    # get all data from database
    db = PostgresApi()
    db.connect_to_db('1podnano&')
    transactions = db.fetch_transactions(31, 1)
    time_sheet = db.fetch_time_sheet(31, 1)
    rented_cars = db.fetch_rented_cars(31, 1)

    # change fuel naming
    for ind, row in transactions.iterrows():
        if row['fuel'].rstrip() == 'АИ-95':
            transactions.iat[ind, 3] = 'Автомоб. бензин АИ-95'
        elif row['fuel'].rstrip() == 'ДТ':
            transactions.iat[ind, 3] = 'Дизельное топливо'

    # assign driver to transaction
    transactions['driver'] = ''      # 5
    for index, row in transactions.iterrows():
        # exclude high refills
        if row['fuel'] == 'АИ-95' and row['liters'] >= 60:
            transactions.iat[index, 5] = 'To tank'
            continue
        elif row['fuel'] == 'Дизельное топливо' and row['liters'] >= 75:
            transactions.iat[index, 5] = 'To tank'
            continue
        else:
            for ind, line in time_sheet.iterrows():
                if line['date'] == row['date'] and \
                        line['entity'].rstrip() == row['entity'].rstrip() and \
                        row['card_number'].rstrip() != '0':
                    transactions.iat[index, 5] = line['acronym']
                    time_sheet = time_sheet.drop(time_sheet.index[ind])
                    time_sheet = time_sheet.reset_index(drop=True)
                    break
    # assign cars to transactions
    transactions['car'] = ''    # 6
    new_rent_list = []

    for ind, line in rented_cars.iterrows():
        for date in daterange(line['rent_start'], line['rent_end']):
            new_rent_list.append([line['plate'], date])

    cars = pd.DataFrame(new_rent_list, columns=['plate', 'date'])

    for index, line in transactions.iterrows():
        for ind, row in cars.iterrows():
            if line['fuel'] == db.fetch_fuel_type(row['plate']).rstrip() and line['date'] == row['date']:
                transactions.iat[index, 6] = row['plate']
                cars = cars.drop(cars.index[ind])
                cars = cars.reset_index(drop=True)
                break

    print(transactions)

    # calculate mileage an fuel for day

    transactions['fuel_start'] = ''     # 7
    transactions['fuel_spent'] = ''     # 8
    transactions['fuel_end'] = ''       # 9
    transactions['mileage_start'] = ''  # 10
    transactions['mileage_today'] = ''  # 11
    transactions['mileage_end'] = ''    # 12

    for index, line in transactions.iterrows():
        tank = db.fetch_tank_volume(line['car'])
        if tank == 0:
            continue

        fuel_start = db.fetch_fuel_end(line['car'])
        fuel_add = transactions.iat[index, 4]
        fuel_spent = rd.uniform(4.21, 5.76)

        transactions.iat[index, 7] = fuel_start

        while True:
            if tank <= fuel_start + fuel_add - fuel_spent:
                diff = tank - fuel_start - fuel_add + fuel_spent
                low = diff - 2.34
                up = diff + 2.37
                fuel_spent += rd.uniform(low, up)
            else:
                transactions.iat[index, 8] = fuel_spent
                transactions.iat[index, 9] = fuel_start + fuel_add - fuel_spent
                break

        transactions.iat[index, 10] = db.fetch_mileage_end(line['car'])
        transactions.iat[index, 11] = db.fetch_consumption(line['car']) * line['fuel_spent']
        transactions.iat[index, 12] = line['mileage_start'] + line['mileage_end']

        mileage_fuel_end = MileageFuelEnd()
        mileage_fuel_end.plate = line['car']
        mileage_fuel_end.date = line['date']
        mileage_fuel_end.fuel_end = line['fuel_end']
        mileage_fuel_end.mileage_end = line['mileage_end']

        db.upload_queries(mileage_fuel_end)

    print(transactions)


if __name__ == '__main__':
    calculate_fuel_consumption()
