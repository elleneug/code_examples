import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class DinaRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/character').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/character?page={page_num}').json()['results']


class DinaRamDeadOrAliveCountOperator(BaseOperator):
    """
    Count number of dead or alive characters
    On DinaRickMortyHook
    """

    template_fields = ('dead_or_alive',)
    ui_color = "#c7ffe9"

    def __init__(self, dead_or_alive: str = 'Dead', **kwargs) -> None:
        super().__init__(**kwargs)
        self.dead_or_alive = dead_or_alive

    def get_dead_or_alive_count_on_page(self, result_json: list) -> int:
        """
        Get count of dead or alive in one page of character
        :param result_json:
        :return: dead_or_alive_count
        """
        dead_or_alive_count_on_page = 0
        for one_char in result_json:
            if one_char.get('status') == self.dead_or_alive:
                dead_or_alive_count_on_page += 1
        logging.info(f'     {self.dead_or_alive} count_on_page = {dead_or_alive_count_on_page}')
        return dead_or_alive_count_on_page

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        hook = DinaRickMortyHook('dina_ram')
        dead_or_alive_count = 0
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            dead_or_alive_count += self.get_dead_or_alive_count_on_page(one_page)
        logging.info(f'{self.dead_or_alive} in Rick&Morty {dead_or_alive_count}')
