import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ESidorovaRamLocationHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_pages_count(self):
        """ Returns count of page in API """
        return self.run("api/location").json()["info"]["pages"]

    def get_location_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class ESidorovaRamLocationOperator(BaseOperator):

    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        hook = ESidorovaRamLocationHook('dina_ram')
        location_list = []
        for page in range(hook.get_pages_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_location_page(str(page + 1))
            for one_char in one_page:
                location_dict = {
                    'id': one_char.get('id'),
                    'name': one_char.get('name'),
                    'type': one_char.get('type'),
                    'dimension': one_char.get('dimension'),
                    'resident_cnt': len(one_char.get('residents'))
                }
                location_list.append(location_dict)
        locations_sorted = sorted(location_list,
                                 key=lambda cnt: cnt['resident_cnt'],
                                 reverse=True)
        top3_location = locations_sorted[:3]
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in top3_location]

        insert_sql = f"INSERT INTO e_sidorova_ram_location VALUES {','.join(insert_values)}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(insert_sql, False)
