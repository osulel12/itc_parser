import json
from class_parser import ITC_parser
from config import conf_dict
from logger_file import logger
import os
from dotenv import load_dotenv


dotenv_path = '.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

dict_postgres_cred = {'user': os.getenv('USER_NAME_PG'),
                      'password': os.getenv('PASSWORD_PG'),
                      'host': os.getenv('HOST_PG'),
                      'port': os.getenv('PORT_PG'),
                      'database': os.getenv('DATABASE_PG')}

if __name__ == '__main__':
    # tariff - значит парсим тарифные линии
    # not_tariff - значит парсим данные на 6 знаках
    variant_parser = 'not_tariff'

    # Imports_error_itc.json - для ошибок в импорте
    # Exports_error_itc.json - для ошибок в экспорте
    file_fixe_name = 'Imports_error_itc.json'

    parser_ex = ITC_parser(dict_html_elements=conf_dict,
                           url_trade_map='https://www.trademap.org/Product_SelCountry_TS.aspx?nvpm=1%7c004%7c%7c%7c%7c%7c122076%7c%7c2%7c1%7c1%7c1%7c2%7c1%7c1%7c1%7c1%7c1',
                           castom_logger=logger('itc_parser_log').create_logger(),
                           chat_id_user=os.getenv(''),
                           bot_token=os.getenv(''),
                           dict_postgres_cred=dict_postgres_cred,
                           patern_file=os.getenv('PATERN_FILE' if variant_parser == 'not_tariff' else 'PATERN_FILE_TARIFF_LINE'),
                           proxy="")

    # 'Imports', 'Exports'
    # 'Values', 'Quantities'
    # ["", "", "", "", ""]
    input_user_text = input("""Чтобы скачать Product cluster at 6 digits введите "6"\nЧтобы скачать Products at the tariff line введите "8"\nЧтобы исправить ошибки допущенные при скачивании введите "0":""", )
    if input_user_text == '6':
        for reporter_name in ["Argentina", "Austria", "Bulgaria", "Brunei Darussalam", "Honduras"]:
            for type_flow in ['Imports', 'Exports']:
                for qty_or_value in ['Values', 'Quantities']:
                    parser_ex.main(reporter_name=reporter_name, type_flow=type_flow, qty_or_value=qty_or_value)

    elif input_user_text == '8':
        for reporter_name in []:
            for type_flow in ['Imports', 'Exports']:
                for qty_or_value in ['Values']:
                    parser_ex.main(reporter_name=reporter_name, type_flow=type_flow, qty_or_value=qty_or_value,
                                   product_cluster=variant_parser)
    elif input_user_text == '0':
        with open(file_fixe_name, encoding='utf-8') as file:
            dict_error = json.load(file)

        parser_ex.main(reporter_name=dict_error['reporter_name'], type_flow=dict_error['type_flow'],
                       qty_or_value='Quantities',
                       product_cluster='not_tariff',
                       partner_list=dict_error['list_partner'])



