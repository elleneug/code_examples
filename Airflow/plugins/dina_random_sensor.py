from random import randrange
import logging

from airflow.sensors.base_sensor_operator import BaseSensorOperator


class DinaRandomSensor(BaseSensorOperator):
    """
    Sensor wait, when random number from 0 to range_number will be equal to zero
    """

    ui_color = "#fffacd"

    def __init__(self, range_number: int = 3, **kwargs) -> None :
        super().__init__(**kwargs)
        self.range_number = range_number

    def poke(self, context):
        """
        :return: if random numer equal to zero
        """
        poke_num = randrange(0, self.range_number)
        logging.info(f'poke: {poke_num}')
        return poke_num == 0
