import pandas as pd
from datetime import datetime
import re


class TimeSheetRow:
    def __init__(self, date=None, t_type=None, hours=None, entity=None, acr=None):
        self.__id = None
        self.__date = date
        self.__type_of_time = t_type
        self.__hours_worked = hours
        self.__entity = entity
        self.__acronym = acr

    @property
    def id(self): return self.__id

    @property
    def acronym(self): return self.__acronym

    @property
    def date(self): return self.__date

    @property
    def type_of_time(self): return self.__type_of_time

    @property
    def hours_worked(self): return self.__hours_worked

    @property
    def entity(self): return self.__entity

    @id.setter
    def id(self, value: str):
        self.__id = value

    @acronym.setter
    def acronym(self, value: str):
        self.__acronym = value

    @date.setter
    def date(self, value: datetime.date):
        self.__date = value

    @type_of_time.setter
    def type_of_time(self, value: str):
        self.__type_of_time = value

    @hours_worked.setter
    def hours_worked(self, value: int):
        self.__hours_worked = value

    @entity.setter
    def entity(self, value: str):
        self.__entity = value


class Worker:

    def __init__(self):
        self.__acronym = None
        self.__eng_name = None
        self.__ru_name = None
        self.__driver_license = None

    @property
    def acronym(self): return self.__acronym

    @property
    def ru_name(self): return self.__ru_name

    @property
    def eng_name(self): return self.__eng_name

    @property
    def driver_license(self): return self.__driver_license

    @acronym.setter
    def acronym(self, value: str):
        assert re.search(r'^[A-Z]{4}$', value)
        self.__acronym = value

    @ru_name.setter
    def ru_name(self, value: str): self.__ru_name = value

    @eng_name.setter
    def eng_name(self, value: str): self.__eng_name = value

    @driver_license.setter
    def driver_license(self, value: bool): self.__driver_license = value


class Car:
    def __init__(self):
        self.__plate = None
        self.__model = None
        self.__vin = None
        self.__owner = None
        self.__fuel = None
        self.__is_special = None
        self.__tank_volume = None
        self.__consumption_rate = None

    @property
    def plate(self): return self.__plate

    @property
    def model(self): return self.__model

    @property
    def vin(self): return self.__vin

    @property
    def owner(self): return self.__owner

    @property
    def fuel(self): return self.__fuel

    @property
    def is_special(self): return self.__is_special

    @property
    def tank_volume(self): return self.__tank_volume

    @property
    def consumption_rate(self): return self.__consumption_rate

    @plate.setter
    def plate(self, value: str):
        pattern = r'^[А-ЯA-Z][0-9]{3}[А-ЯA-Z]{2}[0-9]{3}$'
        assert re.search(pattern, value)
        self.__plate = value

    @model.setter
    def model(self, value: str): self.__model = value

    @vin.setter
    def vin(self, value: str):
        self.__vin = value

    @owner.setter
    def owner(self, value: str): self.__owner = value

    @fuel.setter
    def fuel(self, value: str):
        assert value in ['Автомоб. бензин АИ-95', 'Дизельное топливо']
        self.__fuel = value

    @is_special.setter
    def is_special(self, value: bool): self.__is_special = value

    @tank_volume.setter
    def tank_volume(self, value: int):
        assert value > 30
        self.__tank_volume = value

    @consumption_rate.setter
    def consumption_rate(self, value: float):
        assert value > 5
        value = round(value, 3)
        self.__consumption_rate = value


class RentedCar:
    def __init__(self, plate=None, start=None, end=None):
        self.__id = None
        self.__plate = plate
        self.__rent_start = start
        self.__rent_end = end
        self.__entity = None
        self.__month = None
        self.__year = None

    @property
    def plate(self): return self.__plate

    @property
    def id(self): return self.__id

    @property
    def rent_start(self): return self.__rent_start

    @property
    def rent_end(self): return self.__rent_end

    @property
    def entity(self): return self.__entity

    @property
    def month(self): return self.__month

    @property
    def year(self): return self.__year

    @id.setter
    def id(self, value: int):
        self.__id = value

    @plate.setter
    def plate(self, value: str):
        self.__plate = value

    @rent_start.setter
    def rent_start(self, value: datetime.date):
        self.__rent_start = value

    @rent_end.setter
    def rent_end(self, value: datetime.date):
        self.__rent_end = value

    @entity.setter
    def entity(self, value: str):
        self.__entity = value

    @month.setter
    def month(self, value: int):
        assert 1 <= value <= 12
        self.__month = value

    @year.setter
    def year(self, value: int):
        assert value >= 2022
        self.__year = value


class MileageFuelEnd:
    def __init__(self):
        self.__plate = None
        self.__date = None
        self.__mileage_end = None
        self.__fuel_end = None

    @property
    def plate(self): return self.__plate

    @property
    def date(self): return self.__date

    @property
    def mileage_end(self): return self.__mileage_end

    @property
    def fuel_end(self): return self.__fuel_end

    @plate.setter
    def plate(self, value: str):
        self.__plate = value

    @date.setter
    def date(self, value: datetime.date):
        self.__date = value

    @mileage_end.setter
    def mileage_end(self, value: float):
        self.__mileage_end = value

    @fuel_end.setter
    def fuel_end(self, value: float):
        self.__fuel_end = value


class FuelTransactions:
    def __init__(self):
        self.__id = None
        self.__card_number = None
        self.__date = None
        self.__entity = None
        self.__fuel = None
        self.__liters = None
        self.__price = None
        self.__total_price = None

    def __iter__(self):
        return self

    @property
    def id(self): return self.id

    @property
    def card_number(self): return self.__card_number

    @property
    def date(self): return self.__date

    @property
    def entity(self): return self.__entity

    @property
    def liters(self): return self.__liters

    @property
    def price(self): return self.__price

    @property
    def total_price(self): return self.__total_price

    @property
    def fuel(self): return self.__fuel

    @id.setter
    def id(self, value: int):
        self.__id = value

    @card_number.setter
    def card_number(self, value: str):
        self.__card_number = value

    @date.setter
    def date(self, value: datetime.date):
        self.__date = value

    @entity.setter
    def entity(self, value: str):
        self.__entity = value

    @liters.setter
    def liters(self, value = float):
        self.__liters = value

    @price.setter
    def price(self, value: float):
        self.__price = value

    @total_price.setter
    def total_price(self, value: float):
        self.__total_price = value

    @fuel.setter
    def fuel(self, value: str):
        self.__fuel = value


class WayBill:
    def __init__(self):
        self.__card_number = None
        self.__date = None
        self.__entity = None
        self.__fuel = None
        self.__fuel_get = None
        self.__driver = None
        self.__car = None
        self.__fuel_start = None
        self.__fuel_spent = None
        self.__mileage_start = None
        self.__mileage_spent = None

    @property
    def card_number(self): return self.__card_number

    @card_number.setter
    def card_number(self, value):
        self.__card_number = value

    @property
    def date(self): return self.__date

    @date.setter
    def date(self, value):
        self.__date = value

    @property
    def entity(self): return self.__entity

    @entity.setter
    def entity(self, value):
        self.__entity = value

    @property
    def fuel(self): return self.__fuel

    @fuel.setter
    def fuel(self, value):
        self.__fuel = value

    @property
    def fuel_get(self): return self.__fuel_get

    @fuel_get.setter
    def fuel_get(self, value):
        self.__fuel_get = value

    @property
    def driver(self): return self.__driver

    @driver.setter
    def driver(self, value):
        self.__driver = value

    @property
    def car(self): return self.__car

    @car.setter
    def car(self, value):
        self.__car = value

    @property
    def fuel_start(self): return self.__fuel_start

    @fuel_start.setter
    def fuel_start(self, value):
        self.__fuel_start = value

    @property
    def fuel_spent(self): return self.__fuel_spent

    @fuel_spent.setter
    def fuel_spent(self, value):
        self.__fuel_spent = value

    @property
    def mileage_start(self): return self.__mileage_start

    @mileage_start.setter
    def mileage_start(self, value):
        self.__mileage_start = value

    @property
    def mileage_spent(self): return self.__mileage_spent

    @mileage_spent.setter
    def mileage_spent(self, value):
        self.__mileage_spent = value


class ExpandedRentedCar:
    def __init__(self, plate=None, date=None):
        self.__plate = plate
        self.__date = date

    @property
    def plate(self):
        return self.__plate

    @plate.setter
    def plate(self, value):
        self.__plate = value

    @property
    def date(self):
        return self.__date

    @date.setter
    def date(self, value):
        self.__date = value