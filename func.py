from CsvReader import CsvReader
from DbConnector import PostgresApi
from WayBillFiller import WayBillFiller


def upload_csv(list_of_ints, password):
    names = ['rent_dates',
             'workers',
             'cars',
             'time_sheet',
             'fuel_transactions',
             'mileage_fuel_end']

    for num in list_of_ints:
        element = names[num - 1]
        file = getattr(CsvReader(), element)('csv_data/' + element + '.csv')
        PostgresApi().upload_to_db(file, password)
        print(f'File {element} updated')


def body():
    password = input('Enter password for db: ')
    user_input = ''
    while user_input != '!exit':
        print('What to do:\n1. Upload csv to database\n2. Calculate fuel')
        user_input = input()
        if int(user_input) == 1:
            print('''Which files you want to upload:
                1. Rent dates
                2. Workers
                3. Cars
                4. Time sheet
                5. Fuel transactions
                6. Mileage fuel end
                E.g. 234
            ''')
            num = input()
            upload_csv(list(map(int, str(num))), password)
        elif int(user_input) == 2:
            year = int(input('Enter year: '))
            month = int(input('Enter month: '))
            filler = WayBillFiller(password, month, year)
            filler.fill()
            filler.upload()
        else:
            print('Wrong input')
