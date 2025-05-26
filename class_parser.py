import logging
import os
import time
import typing
import json
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException as no_element
from selenium.common.exceptions import StaleElementReferenceException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import psycopg2
import telebot


class ITC_parser:

    def __init__(self, dict_html_elements: dict, url_trade_map: str, castom_logger: logging.Logger,
                 chat_id_user: str, bot_token: str, dict_postgres_cred: dict, patern_file: str,
                 proxy: str):
        """
        :param dict_html_elements: словарь с тэгами для навигации по сайту

        :param url_trade_map: ссылка для загрузки сайта

        :param castom_logger: экзепляр класса logging для записи логов в файл

        :param chat_id_user: chat_id пользователя, который запускает скрипт

        :param bot_token: токен бота в который будут приходить уведомления

        :param dict_postgres_cred: креды для подключения к базе данных

        :param patern_file: патерн названия файла для проверки, скачен он или нет

        :param proxy: прокси для запуска парсера, чтобы не забанили основной ip
        """
        self.dict_html_elements = dict_html_elements
        self.url_trade_map = url_trade_map
        self.castom_logger = castom_logger
        self.chat_id_user = chat_id_user
        self.bot = telebot.TeleBot(bot_token)
        self.postgres_conn = psycopg2.connect(user=dict_postgres_cred['user'],
                                              password=dict_postgres_cred['password'],
                                              host=dict_postgres_cred['host'],
                                              port=dict_postgres_cred['port'],
                                              database=dict_postgres_cred['database'])
        self.patern_file = patern_file
        self.proxy = proxy

    def insert_user_in_db(self):
        """
        Добавляем chat_id_user в базу данных

        :return:
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute("""INSERT INTO ce (chat_id)
                           VALUES (%s)""", (self.chat_id_user,))
        self.postgres_conn.commit()

    def login(self, browser: webdriver, flag_insert_user: bool):
        """
        Функция отвечает за вход в учетную запись на сайте

        :param browser: экземпляр класса webdriver через который мы сделали запрос к сайту

        :param flag_insert_user: флаг отвечающий за добавление нового пользователя (того, кто впервые указала свой id
                                 для алертинга)

        :return:
        """

        # Кликаем по кнопке авторизации
        WebDriverWait(browser, 10).until(
            EC.element_to_be_clickable((By.ID, self.dict_html_elements['authorization_form']))) \
            .click()

        # Находим поле для ввода логина
        WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.ID, self.dict_html_elements['user_form_field']))) \
            .send_keys(self.dict_html_elements['mail_user'])

        # Находим поле для ввода пароля
        WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.ID, self.dict_html_elements['pass_form_field']))) \
            .send_keys(self.dict_html_elements['pswd_user'])

        # Находим на странице кнопку отправки формы и кликаем по ней
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.NAME, self.dict_html_elements['login_button']))) \
            .click()

        if flag_insert_user:
            self.insert_user_in_db()

    def get_captcha_flag(self) -> bool:
        """
        Возвращает флаг, который отвечает за актуальность капчи,
        True - значит значение капчи введено пользователем и актуально для текущей сессии
        False - значит значение уже не актуально для текущей сессии

        :return: флаг отвечающий за актуальность капчи
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute('SELECT captcha_flag FROM ce WHERE chat_id = %s',
                        (self.chat_id_user,))
            return cur.fetchone()[0]

    def get_captcha_text(self) -> str:
        """
        :return: текст введенной капчи
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute('SELECT captcha_text FROM ce WHERE chat_id = %s',
                        (self.chat_id_user,))
            return cur.fetchone()[0]

    def update_captcha_flag(self):
        """
        Обновление статуса капчи на False после того, как мы ввели ее на сайте

        :return:
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute('UPDATE ce SET captcha_flag = False WHERE chat_id = %s',
                        (self.chat_id_user,))
        self.postgres_conn.commit()

    def update_partner_flag(self):
        """
        Обновление partner_flag на False после того, как сформировали список

        :return:
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute('UPDATE ce SET partner_flag = False WHERE chat_id = %s',
                        (self.chat_id_user,))
        self.postgres_conn.commit()

    def update_current_partner(self, partner: str):
        """
        Обновляет название партнера в базе данных на того, с которым сейчас работаем

        :param partner: партнер, который идет текущим в цикле

        :return:
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute("""UPDATE ce SET current_partner = %s WHERE chat_id = %s""",
                        (partner, self.chat_id_user))
        self.postgres_conn.commit()

    def get_partner_save_point(self) -> tuple:
        """
        :return: Возвращает кортеж состоящий из названия партнера и флага.
                 Флаг - задает поведение сборки списка партнеров, если True, то список будет собран начиная с
                        возвращенного партнера, если false, то список будет собран полностью
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute("""SELECT current_partner, partner_flag FROM ce WHERE chat_id = %s""",
                        (self.chat_id_user,))
            return cur.fetchone()

    def update_captha_message_id(self, message_id: str):
        """
        Обновляет id отправленной капчи

        :param message_id: id капчи в чате

        :return:
        """
        with self.postgres_conn.cursor() as cur:
            cur.execute("""UPDATE ce SET captha_message_id = %s WHERE chat_id = %s""",
                        (message_id, self.chat_id_user))
        self.postgres_conn.commit()

    def click_button_yearly_time_series(self, browser: webdriver, type_flow: str, reporter_name: str = None) -> bool:
        """
        Прокликиваем кнопки для того, чтобы перейти в раздел сайта с данными.
        Используется в случае с капчей или если во время сбора данных случилась ошибка

        :param browser: экземпляр класса webdriver

        :param type_flow: тип операции (импорт или экспорт)

        :param reporter_name: текущая страна репортер

        :return: булево значение, успешно или нет мы прокликали данный раздел
        """
        try:
            # Используется тогда, когда передан репортер (в основном для обработки ошибок)
            if reporter_name:
                # Прокликиваем продукт
                WebDriverWait(browser, 7).until(
                    EC.element_to_be_clickable(
                        (By.ID, self.dict_html_elements['delete_product']))).click()

                WebDriverWait(browser, 7).until(
                    EC.element_to_be_clickable(
                        (By.ID, self.dict_html_elements['box_product']))).click()

                WebDriverWait(browser, 7).until(
                    EC.presence_of_element_located((By.XPATH,
                                                    f"""//div[@id='{self.dict_html_elements['product_drop_down_placeholder']}']
                                                    /div[@id='{self.dict_html_elements['product_drop_down']}']/div[@id='{self.dict_html_elements['product_c0']}']"""))) \
                    .click()

                # Прокликиваем страну репортера
                WebDriverWait(browser, 7).until(
                    EC.element_to_be_clickable(
                        (By.ID, self.dict_html_elements['delete_country']))).click()

                WebDriverWait(browser, 7).until(
                    EC.visibility_of_element_located(
                        (By.ID, self.dict_html_elements['box_country_input']))).send_keys(reporter_name)

                WebDriverWait(browser, 7).until(
                    EC.presence_of_element_located((By.XPATH,
                                                    f"""//div[@id='{self.dict_html_elements['country_drop_down_placeholder']}']
                                                                    /div[@id='{self.dict_html_elements['country_drop_down']}']/div[contains(text(), "{reporter_name}")]"""))) \
                    .click()

            # Используется всегда
            WebDriverWait(browser, 7).until(
                EC.element_to_be_clickable(
                    (By.ID, self.dict_html_elements['type_button_import'] if type_flow == 'Imports'
                    else self.dict_html_elements['type_button_export']))).click()

            WebDriverWait(browser, 7) \
                .until(EC.element_to_be_clickable(
                (By.ID, self.dict_html_elements['type_button_Yearly_Time_Series']))).click()
            self.castom_logger.info(f'КАПЧА ПРОЙДЕНА')
            return True
        except TimeoutException:
            # Случай, когда после ввода капчи на странице есть элемент country_reporter тоже считается
            # Как успешный проход капчи
            try:
                WebDriverWait(browser, 7).until(EC.presence_of_element_located(
                    (By.ID, self.dict_html_elements['country_reporter'])))
                self.castom_logger.info('ПРОШЛИ МИНУЯ Home & Search')
                return True
            except TimeoutException:
                return False

    def check_captcha(self, browser: webdriver, type_flow: str, reporter_name: typing.Optional[str] = None) -> bool:
        """
        Функция проверяет наличие капчи и если она есть и введена правильно проходит ее,
        в противном случае, отправляет новый текст капчи и ждет, пока пользователь обновит ее

        :param browser: экземпляр класса webdriver запущенный в текущей сессии

        :param type_flow: направление торговли

        :param reporter_name: имя репортера (опционально)

        :return: булево значение сообщающее о проходе капчи
        """
        try:
            captch = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'div_captchaImg')))
            captch.screenshot('captcha_picture.png')
            with open('captcha_picture.png', 'rb') as captha:
                message = self.bot.send_photo(self.chat_id_user, captha, caption='‼️ Введите текст капчи!')
                self.update_captha_message_id(message.photo[-1].file_id)
                time.sleep(45)
            while True:
                if self.get_captcha_flag():
                    captcha_text = self.get_captcha_text()
                    browser.find_element(By.ID, 'ctl00_PageContent_CaptchaAnswer').send_keys(captcha_text)
                    browser.find_element(By.ID, 'ctl00_PageContent_ButtonvalidateCaptcha').click()
                    self.castom_logger.info(f'ТЕКСТ КАПЧИ {captcha_text}')
                    self.update_captcha_flag()

                    # Проверяем, пройдена капча или нет и проходим на страницу с даными
                    return self.click_button_yearly_time_series(browser, type_flow, reporter_name)
                else:
                    self.castom_logger.info('ОЖИДАЕМ ВВОД КАПЧИ 30 секунд')
                    time.sleep(30)
        except TimeoutException as e:
            self.castom_logger.info(f'ОШИБКА {e}. Вход без капчи')
            return True

    def option_check(self, browser: webdriver, type_flow: str, product_cluster_level_text: str, measure_type: str,
                     reporter_name: str):
        """
        Проверка опций перед скачиванием

        :param browser: экземпляр класса webdriver запущенный в текущей сессии

        :param type_flow: направление торговли

        :param product_cluster_level_text: на каком знаке выбираем продукты (или тарифной линии)

        :param measure_type: проверяем деньги или вес

        :param reporter_name: имя репортера, для проверки

        :return:
        """
        # Проверяем репортера
        try:
            reporter_params = WebDriverWait(browser, 3).until(
                EC.presence_of_element_located((By.ID, self.dict_html_elements['country_reporter'])))
            if reporter_params.get_attribute('value') == reporter_name:
                self.castom_logger.info('* Reporter')
            else:
                raise TimeoutException
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['country_reporter']}']/option[text()="{reporter_name}"]""").click()
            self.castom_logger.info('ПОПРАВЛЯЕМ Reporter')

        # Проверка trade_type (импорт или экспорт)
        try:
            WebDriverWait(browser, 3).until(
                EC.text_to_be_present_in_element_value((By.ID, self.dict_html_elements['trade_type']),
                                                       type_flow[0]))
            self.castom_logger.info(f'** Trade_type')
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['trade_type']}']/option[text()="{type_flow}"]""").click()
            self.castom_logger.info(f'ПОПРАВЛЯЕМ Trade_type')

        # Проверка блока с Yearly time series
        try:
            WebDriverWait(browser, 3).until(
                EC.text_to_be_present_in_element_value((By.ID, self.dict_html_elements['output_type']),
                                                       'TSY'))
            self.castom_logger.info(f'*** Yearly time series')
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['output_type']}']/option[text()='Yearly time series']""").click()
            self.castom_logger.info(f'ПОПРАВЛЯЕМ Yearly time series')

        # Проверка блока с by product
        try:
            WebDriverWait(browser, 3).until(EC.text_to_be_present_in_element_value((By.ID,
                                                                                    self.dict_html_elements[
                                                                                        "output_option"]),
                                                                                   'ByProduct'))
            self.castom_logger.info(f'**** By product')
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"//select[@id='{self.dict_html_elements['output_option']}']/option[text()='by product']").click()
            self.castom_logger.info(f'ПОПРАВЛЯЕМ By product')

        # Проверка на количество выбранных занков (6 значный код или выбрана тарифная линия)
        try:
            WebDriverWait(browser, 3).until(EC.text_to_be_present_in_element_value((By.ID,
                                                                                    self.dict_html_elements[
                                                                                        'product_cluster_level']),
                                                                                   '6' if product_cluster_level_text == 'Product cluster at 6 digits' else '8'))
            self.castom_logger.info(f'***** Product cluster')
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['product_cluster_level']}']/option[text()="{product_cluster_level_text}"]""").click()
            self.castom_logger.info(f'ПОПРАВЛЯЕМ Product cluster')

        # Проверка блока Выбор валюта или вес
        try:
            WebDriverWait(browser, 3).until(EC.text_to_be_present_in_element_value((By.ID,
                                                                                    self.dict_html_elements[
                                                                                        'ts_indicator']),
                                                                                   measure_type[0]))
            self.castom_logger.info(f'****** Value_or_Qty')
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['ts_indicator']}']/option[text()="{measure_type}"]""").click()
            self.castom_logger.info(f'ПОПРАВЛЯЕМ Value_or_Qty')

        # Проверка типа валюты
        if measure_type == 'Values':
            try:
                WebDriverWait(browser, 3).until(EC.text_to_be_present_in_element_value((By.ID,
                                                                                        self.dict_html_elements[
                                                                                            'ts_currency']),
                                                                                       'USD'))
                self.castom_logger.info(f'+ USD')
            except TimeoutException:
                browser.find_element(
                    By.XPATH,
                    f"//select[@id='{self.dict_html_elements['ts_currency']}']/option[text()='US Dollar']").click()
                self.castom_logger.info(f'ПОПРАВЛЯЕМ USD')

        # Выбираем кол-во лет
        try:
            WebDriverWait(browser, 3).until(EC.text_to_be_present_in_element_value((By.ID,
                                                                                    self.dict_html_elements[
                                                                                        'num_time_period']),
                                                                                   '12'))
            self.castom_logger.info(f'******* Per page')
        except TimeoutException:
            browser.find_element(
                By.XPATH,
                f"//select[@id='{self.dict_html_elements['num_time_period']}']/option[text()='12 per page']").click()
            self.castom_logger.info(f'ПОПРАВЛЯЕМ Per page')

    @staticmethod
    def _json_work_file(flag_type_work: str, file: str, reporter_country: str, value: str = '') -> list:
        """
        Записываем данные в json файлы или редактируем их

        :param flag_type_work: флаг, от него зависит какой тип операции мы будем делать

        :param file: название файла

        :param reporter_country: название репортера

        :param value: ззачастую название партнера

        :return: при флаге return_save_point список стран партнеров для переданного репортера
        """
        if flag_type_work == 'other':
            if os.path.isfile(file):
                with open(file, encoding='utf-8') as fl:
                    js_dict = json.load(fl)
                if reporter_country in js_dict:
                    if value not in js_dict[reporter_country]:
                        js_dict[reporter_country].append(value)
                else:
                    js_dict[reporter_country] = [value]
                with open(file, 'w', encoding='utf-8') as fl:
                    json.dump(js_dict, fl, indent=4, ensure_ascii=False)
            else:
                dct = {reporter_country: [value]}
                with open(file, 'w', encoding='utf-8') as fl:
                    json.dump(dct, fl, indent=4, ensure_ascii=False)
        elif flag_type_work == 'del_value':
            if os.path.isfile(file):
                with open(file, encoding='utf-8') as fl:
                    js_dict = json.load(fl)
                if reporter_country in js_dict:
                    try:
                        js_dict[reporter_country].remove(value)
                        with open(file, 'w', encoding='utf-8') as fl:
                            json.dump(js_dict, fl, indent=4, ensure_ascii=False)
                    except ValueError:
                        pass
        elif flag_type_work == 'return_save_point':
            with open(file, encoding='utf-8') as fl:
                js_dict = json.load(fl)
            return js_dict[reporter_country]

    def processing_log_out_exception(self, browser: webdriver, type_flow: str, reporter_name: str):

        # Если появляется новостное окно
        while True:
            try:
                WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                            'ctl00_MenuControl_CheckBox_DoNotShowAgain'))).click()
                WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                            'ctl00_MenuControl_button1'))).click()
                self.castom_logger.info('ЗАКРЫЛИ НОВОСТНОЕ ОКНО')
            except (TimeoutException, StaleElementReferenceException) as e:
                if isinstance(e, StaleElementReferenceException):
                    self.bot.send_message(self.chat_id_user, 'ОШБИКА С НОВОСТНЫМ ОКНОМ')
                    time.sleep(60)
                else:
                    self.castom_logger.info('НОВОСТНОЕ ОКНО  ОТСУТСТВУЕТ')
                    break

        try:
            WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                    'ctl00_MenuControl_button_CloseRestrictedPopup_Bottom'))).click()
        except TimeoutException:
            self.castom_logger.info('iframe ОТСУТСТВУЕТ')

        try:
            try:
                self.login(browser, False)
            except TimeoutException:
                self.castom_logger.info('АВТОРИЗАЦИЯ БЕЗ ЛОГИНА И ПАРОЛЯ')
            WebDriverWait(browser, 7).until(EC.presence_of_element_located((By.CLASS_NAME, 'div_captchaImg')))
            while True:
                if self.check_captcha(browser, type_flow, reporter_name):
                    self.bot.send_message(self.chat_id_user, f"""✅ Капча для <b>{reporter_name} {type_flow}</b> успешно пройдена""",
                                          parse_mode='html')
                    break
                else:
                    self.bot.send_message(self.chat_id_user,
                                          f"""❌ Капча для <b>{reporter_name} {type_flow} не пройдена</b>. Введите ее повторно после получения обновленной картинки""",
                                          parse_mode='html')
        except TimeoutException:
            rez_click_button = self.click_button_yearly_time_series(browser, type_flow, reporter_name)
            self.castom_logger.info(f'{"ОБРАБОТКА ОШИБКИ ЧЕРЕЗ yearly_time_series" if rez_click_button else "ТОЛЬКО ЛОГИН"}')

    def downloading_trade_value(self, browser: webdriver, type_flow: str, reporter_name: str, full_path_download: str,
                                flag_insert_user: typing.Optional[bool] = False):
        """
        Скачиваем данные для Value

        :param browser: экземпляр класса webdriver

        :param type_flow: направление торговли (импорт или экспорт)

        :param reporter_name: имя репортера

        :param full_path_download: полный путь, куда будет скачиваться файл

        :param flag_insert_user: флаг отвчающий, будет пользователь запускающий скрипт записан в базу или нет
                                 (нужно при первом запуске скрипта пользователем)

        :return:
        """

        reporter_name_for_check = reporter_name.replace(',', ' ').replace(' ', '_')
        # Меняем в зависимости от нужной длины кода
        product_cluster_level_text = 'Product cluster at 6 digits'

        browser.get(self.url_trade_map)

        # Если появляется новостное окно
        while True:
            try:
                WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                            'ctl00_MenuControl_CheckBox_DoNotShowAgain'))).click()
                WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                            'ctl00_MenuControl_button1'))).click()
                self.castom_logger.info('ЗАКРЫЛИ НОВОСТНОЕ ОКНО')
            except TimeoutException:
                self.castom_logger.info('НОВОСТНОЕ ОКНО  ОТСУТСТВУЕТ')
                break

        try:
            self.login(browser, flag_insert_user)
        except TimeoutException as e:
            self.castom_logger.info(f'АВТОРИЗАЦИЯ БЕЗ ВВОДА Л.П. {e}')
        while True:
            if self.check_captcha(browser, type_flow):
                self.bot.send_message(self.chat_id_user, f'✅ Капча для <b>{reporter_name} Values {type_flow}</b> успешно пройдена',
                                      parse_mode='html')
                break
            else:
                self.bot.send_message(self.chat_id_user,
                                      f'❌ Капча для <b>{reporter_name} Values {type_flow}</b> не пройдена. Введите ее повторно после получения обновленной картинки',
                                      parse_mode='html')
        try:
            # Выбираем нужного репортера
            country_reporter = browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['country_reporter']}']/option[text()="{reporter_name}"]""")
            country_reporter.click()
            self.castom_logger.info(f"""РЕПОРТЕР {reporter_name}""")
        except no_element as e:
            self.castom_logger.info(f"""ОШИБКА ПРИ ВЫБОРЕ РЕПОРТЕРА {reporter_name} {e}""")
            browser.get(self.url_trade_map)
            self.processing_log_out_exception(browser, type_flow, reporter_name)
            # Выбираем нужного репортера
            country_reporter = browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['country_reporter']}']/option[text()="{reporter_name}"]""")
            country_reporter.click()
            self.castom_logger.info(f"""РЕПОРТЕР {reporter_name}""")

        # Проверка на зеркальные данные и дальнейшая фильтрация этих годов при финальной сборке
        year_mirror_data = browser.find_element(By.XPATH,
                                                self.dict_html_elements['year_mirror_xpath']).find_elements(
            By.TAG_NAME, 'th')[3:15]
        value_mirror_data = browser.find_element(By.XPATH,
                                                 self.dict_html_elements['value_mirror_xpath']).find_elements(
            By.TAG_NAME, 'td')[3:15]

        for year_mirror, value_mirror in zip(year_mirror_data, value_mirror_data):
            if value_mirror.get_attribute('title') == 'Mirror data':
                excluded_year = year_mirror.text.split(' in ')[1]
                self.castom_logger.info(f"MIRROR DATA {excluded_year} году для {reporter_name}")
                self._json_work_file('other', 'json_mirror_data.json', reporter_name, excluded_year)
            else:
                excluded_year = year_mirror.text.split(' in ')[1]
                self._json_work_file('del_value', 'json_mirror_data.json', reporter_name,
                                     excluded_year)

        parnter_in_bd, flag_partner = self.get_partner_save_point()
        partner_list = [partner.text for partner in
                        browser.find_element(By.ID, self.dict_html_elements['country_partner']).find_elements(
                            By.TAG_NAME, 'option') if partner.text != 'All']
        if flag_partner:
            self.castom_logger.info(f"""НАЧИНАЕМ СПИСОК С {parnter_in_bd}""")
            partner_list = partner_list[partner_list.index(parnter_in_bd):]
            self.update_partner_flag()

        for partner in partner_list:
            # Записываем текущего партнера в базу данных
            self.update_current_partner(partner)
            while True:
                try:
                    browser.find_element(
                        By.XPATH,
                        f"""//select[@id='{self.dict_html_elements['country_partner']}']/option[text()="{partner}"]""").click()
                    self.castom_logger.info(f"""ПАРТНЕР {partner}. РЕПОРТЕРА {reporter_name}""")
                    # measure_type - подставить в зависимости от типа измерение 'Values', 'Quantities'
                    self.option_check(browser, type_flow, product_cluster_level_text, 'Values', reporter_name)

                    partner_text_for_check = partner.replace(',', ' ').replace(' ', '_')
                    file_name = self.patern_file.format(reporter_name_for_check, partner_text_for_check)
                    upper_border = self.dict_html_elements['world_upper_border'] if partner == 'World' else \
                        self.dict_html_elements['country_upper_border']

                    check_zero_country = sum([int(i.text.replace(',', '')) for i in
                                              browser.find_element(By.XPATH, self.dict_html_elements['zero_check']) \
                                             .find_elements(By.TAG_NAME, 'td') \
                                                  [
                                              3:int(browser.find_element(By.ID, upper_border).get_attribute(
                                                  'colspan')) + 3]
                                              if i.text.replace(',', '').isdigit()])
                    check_downloaad_file = os.path.isfile(os.path.join(full_path_download, file_name))
                    if check_zero_country > 0 and not check_downloaad_file:
                        self.castom_logger.info(f"""СКАЧИВАЕМ ФАЙЛ {file_name}""")
                        WebDriverWait(browser, 15).until(EC.element_to_be_clickable(
                            (By.ID, self.dict_html_elements['download_button_txt']))).click()

                        # Проверяем скачен ли файл
                        start_time = time.time()
                        flag_file_download = False
                        while time.time() - start_time < 180:
                            if os.path.isfile(os.path.join(full_path_download, file_name)):
                                self._json_work_file('other', f'{type_flow}_res.json',
                                                     reporter_name, partner)
                                self.castom_logger.info(f"""ФАЙЛ {file_name} ЗАГРУЖЕН!""")
                                flag_file_download = True
                                break
                            else:
                                self.castom_logger.info(f"""ОЖИДАЕМ ЗАГРУЗКУ {file_name}""")
                                time.sleep(1)
                        if flag_file_download:
                            break
                        else:
                            continue
                    elif check_zero_country == 0:
                        self.castom_logger.info(f"""ДАННЫХ НЕТ ПО {partner} """)
                        # Удаляем не подходящую страну(если делаем парсинг повторно)
                        self._json_work_file('del_value', f'{type_flow}_res.json',
                                             reporter_name, partner)
                        break
                    elif os.path.isfile(os.path.join(full_path_download, file_name)):
                        self.castom_logger.info(f"""УЖЕ ЗАГРУЖЕН {file_name} !""")
                        break
                except (no_element, TimeoutException, StaleElementReferenceException,
                        ElementClickInterceptedException) as e:
                    self.castom_logger.info(f"""ОШИБКА для {reporter_name} по {partner} {e}""")
                    browser.get(self.url_trade_map)
                    self.processing_log_out_exception(browser, type_flow, reporter_name)

    def downloading_quantities(self, browser: webdriver, type_flow: str, reporter_name: str, full_path_download: str,
                               partner_list: typing.Optional[list] = None,
                               flag_insert_user: typing.Optional[bool] = False):
        """
        Скачиваем данные для quantities

        :param browser: экземпляр класса webdriver

        :param type_flow: направление торговли (импорт или экспорт)

        :param reporter_name: имя репортера

        :param full_path_download: полный путь, куда будет скачиваться файл

        :param partner_list: список партнеров, для исправления ошибок (опционально)

        :param flag_insert_user: флаг отвчающий, будет пользователь запускающий скрипт записан в базу или нет
                                 (нужно при первом запуске скрипта пользователем)

        :return:
        """

        reporter_name_for_check = reporter_name.replace(',', ' ').replace(' ', '_')
        # Меняем в зависимости от нужной длины кода
        product_cluster_level_text = 'Product cluster at 6 digits'

        browser.get(self.url_trade_map)

        # Если появляется новостное окно
        while True:
            try:
                WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                            'ctl00_MenuControl_CheckBox_DoNotShowAgain'))).click()
                WebDriverWait(browser, 7).until(EC.element_to_be_clickable((By.ID,
                                                                            'ctl00_MenuControl_button1'))).click()
                self.castom_logger.info('ЗАКРЫЛИ НОВОСТНОЕ ОКНО')
            except TimeoutException:
                self.castom_logger.info('НОВОСТНОЕ ОКНО  ОТСУТСТВУЕТ')
                break

        try:
            self.login(browser, flag_insert_user)
        except TimeoutException as e:
            self.castom_logger.info(f'АВТОРИЗАЦИЯ БЕЗ ВВОДА Л.П. {e}')
        while True:
            if self.check_captcha(browser, type_flow):
                self.bot.send_message(self.chat_id_user, f"""✅ Капча для <b>{reporter_name} Quantities {type_flow}</b> успешно пройдена""",
                                      parse_mode='html')
                break
            else:
                self.bot.send_message(self.chat_id_user,
                                      f"""❌ Капча для <b>{reporter_name} Quantities {type_flow}</b> не пройдена. Введите ее повторно после получения обновленной картинки""",
                                      parse_mode='html')

        try:
            # Выбираем нужного репортера
            country_reporter = browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['country_reporter']}']/option[text()="{reporter_name}"]""")
            country_reporter.click()
            self.castom_logger.info(f"""РЕПОРТЕР {reporter_name}""")
        except no_element as e:
            self.castom_logger.info(f"""ОШИБКА ПРИ ВЫБОРЕ РЕПОРТЕРА {reporter_name} {e}""")
            browser.get(self.url_trade_map)
            self.processing_log_out_exception(browser, type_flow, reporter_name)

        if partner_list:
            self.castom_logger.info(f"""ПОВТОРНАЯ ЗАГРУЗКА {partner_list}""")
        else:
            parnter_in_bd, flag_partner = self.get_partner_save_point()
            partner_list = self._json_work_file('return_save_point', f'{type_flow}_res.json',
                                                reporter_name)
            if flag_partner:
                self.castom_logger.info(f"""НАЧИНАЕМ СПИСОК С {parnter_in_bd}""")
                partner_list = partner_list[partner_list.index(parnter_in_bd):]
                self.update_partner_flag()

        for partner in partner_list:
            # Записываем текущего партнера в базу данных
            self.update_current_partner(partner)
            while True:
                try:
                    browser.find_element(
                        By.XPATH,
                        f"""//select[@id='{self.dict_html_elements['country_partner']}']/option[text()="{partner}"]""").click()
                    self.castom_logger.info(f"""ПАРТНЕР {partner}. РЕПОРТЕР {reporter_name}""")
                    # measure_type - подставить в зависимости от типа измерение 'Values', 'Quantities'
                    self.option_check(browser, type_flow, product_cluster_level_text, 'Quantities', reporter_name)

                    partner_text_for_check = partner.replace(',', ' ').replace(' ', '_')
                    file_name = self.patern_file.format(reporter_name_for_check, partner_text_for_check)

                    check_downloaad_file = os.path.isfile(os.path.join(full_path_download, file_name))
                    if not check_downloaad_file:
                        self.castom_logger.info(f"""СКАЧИВАЕМ ФАЙЛ {file_name}""")
                        WebDriverWait(browser, 15).until(EC.element_to_be_clickable(
                            (By.ID, self.dict_html_elements['download_button_txt']))).click()

                        # Проверяем скачен ли файл
                        start_time = time.time()
                        flag_file_download = False
                        while time.time() - start_time < 180:
                            if os.path.isfile(os.path.join(full_path_download, file_name)):
                                self.castom_logger.info(f"""ФАЙЛ {file_name} ЗАГРУЖЕН!""")
                                flag_file_download = True
                                break
                            else:
                                self.castom_logger.info(f"""ОЖИДАЕМ ЗАГРУЗКУ {file_name}""")
                                time.sleep(1)
                        if flag_file_download:
                            break
                        else:
                            continue
                    elif os.path.isfile(os.path.join(full_path_download, file_name)):
                        self.castom_logger.info(f"""УЖЕ ЗАГРУЖЕН {file_name} !""")
                        break
                except (no_element, TimeoutException, StaleElementReferenceException,
                        ElementClickInterceptedException) as e:
                    self.castom_logger.info(f"""ОШИБКА для {reporter_name} по {partner} {e}""")
                    browser.get(self.url_trade_map)
                    self.processing_log_out_exception(browser, type_flow, reporter_name)

    def downloading_tariff_line_value(self, browser: webdriver, type_flow: str, reporter_name: str,
                                      full_path_download: str,
                                      flag_insert_user: typing.Optional[bool] = False):
        """
        Скачиваем данные для quantities

        :param browser: экземпляр класса webdriver

        :param type_flow: направление торговли (импорт или экспорт)

        :param reporter_name: имя репортера

        :param full_path_download: полный путь, куда будет скачиваться файл


        :param flag_insert_user: флаг отвчающий, будет пользователь запускающий скрипт записан в базу или нет
                                 (нужно при первом запуске скрипта пользователем)

        :return:
        """

        reporter_name_for_check = reporter_name.replace(',', ' ').replace(' ', '_')
        # Меняем в зависимости от нужной длины кода
        product_cluster_level_text = 'Products at the tariff line'

        browser.get(self.url_trade_map)
        try:
            self.login(browser, flag_insert_user)
        except TimeoutException as e:
            self.castom_logger.info(f'АВТОРИЗАЦИЯ БЕЗ ВВОДА Л.П. {e}')
        while True:
            if self.check_captcha(browser, type_flow):
                self.bot.send_message(self.chat_id_user, f"""✅ Капча для <b>{reporter_name} tariff line Values {type_flow}</b> успешно пройдена""",
                                      parse_mode='html')
                break
            else:
                self.bot.send_message(self.chat_id_user,
                                      f"""❌ Капча для <b>{reporter_name} tariff line Values {type_flow}</b> не пройдена. Введите ее повторно после получения обновленной картинки""",
                                      parse_mode='html')

        try:
            # Выбираем нужного репортера
            country_reporter = browser.find_element(
                By.XPATH,
                f"""//select[@id='{self.dict_html_elements['country_reporter']}']/option[text()="{reporter_name}"]""")
            country_reporter.click()
            self.castom_logger.info(f"""РЕПОРТЕР {reporter_name}""")
        except no_element as e:
            self.castom_logger.info(f"""ОШИБКА ПРИ ВЫБОРЕ РЕПОРТЕРА {reporter_name} {e}""")
            browser.get(self.url_trade_map)
            self.processing_log_out_exception(browser, type_flow, reporter_name)

        parnter_in_bd, flag_partner = self.get_partner_save_point()
        # partner_list = self._json_work_file('return_save_point', f'{type_flow}_res.json',
        #                                     reporter_name)
        partner_list = [partner.text for partner in
                        browser.find_element(By.ID, self.dict_html_elements['country_partner']).find_elements(
                            By.TAG_NAME, 'option') if partner.text != 'All']
        if flag_partner:
            self.castom_logger.info(f"""НАЧИНАЕМ СПИСОК С {parnter_in_bd}""")
            partner_list = partner_list[partner_list.index(parnter_in_bd):]
            self.update_partner_flag()

        for partner in partner_list:
            # Записываем текущего партнера в базу данных
            self.update_current_partner(partner)
            while True:
                try:
                    browser.find_element(
                        By.XPATH,
                        f"""//select[@id='{self.dict_html_elements['country_partner']}']/option[text()="{partner}"]""").click()
                    self.castom_logger.info(f"""ПАРТНЕР {partner}. РЕПОРТЕР {reporter_name}""")
                    # measure_type - подставить в зависимости от типа измерение 'Values', 'Quantities'
                    self.option_check(browser, type_flow, product_cluster_level_text, 'Values', reporter_name)

                    partner_text_for_check = partner.replace(',', ' ').replace(' ', '_')
                    file_name = self.patern_file.format(reporter_name_for_check, partner_text_for_check)

                    check_downloaad_file = os.path.isfile(os.path.join(full_path_download, file_name))
                    if not check_downloaad_file:
                        self.castom_logger.info(f"""СКАЧИВАЕМ ФАЙЛ {file_name}""")
                        WebDriverWait(browser, 15).until(EC.element_to_be_clickable(
                            (By.ID, self.dict_html_elements['download_button_txt']))).click()

                        # Проверяем скачен ли файл
                        start_time = time.time()
                        flag_file_download = False
                        while time.time() - start_time < 180:
                            if os.path.isfile(os.path.join(full_path_download, file_name)):
                                self.castom_logger.info(f"""ФАЙЛ {file_name} ЗАГРУЖЕН!""")
                                flag_file_download = True
                                break
                            else:
                                self.castom_logger.info(f"""ОЖИДАЕМ ЗАГРУЗКУ {file_name}""")
                                time.sleep(1)
                        if flag_file_download:
                            break
                        else:
                            continue
                    elif os.path.isfile(os.path.join(full_path_download, file_name)):
                        self.castom_logger.info(f"""УЖЕ ЗАГРУЖЕН {file_name} !""")
                        break
                except (no_element, TimeoutException, StaleElementReferenceException,
                        ElementClickInterceptedException) as e:
                    self.castom_logger.info(f"""ОШИБКА для {reporter_name} по {partner} {e}""")
                    browser.get(self.url_trade_map)
                    self.processing_log_out_exception(browser, type_flow, reporter_name)

    def main(self, reporter_name: str, type_flow: str, qty_or_value: str, partner_list: typing.Optional[list] = None,
             product_cluster: typing.Optional[str] = 'not_tariff', flag_insert_user: typing.Optional[bool] = False):
        """
        Основная функция которая вызывает необходимые функции для скачиваня данных

        :param reporter_name: репортер с которым сейчас работаем

        :param type_flow: направление торговли (Exports Imports)

        :param qty_or_value: какая величина сейчас скачивается (Quantities Values)

        :param partner_list: список партнеров, для исправления ошибок (опционально)

        :param product_cluster: используется для парсинга тарифов. Участвует в формировании названия папки

        :param flag_insert_user: флаг отвчающий, будет пользователь запускающий скрипт записан в базу или нет
                                 (нужно при первом запуске скрипта пользователем)

        :return:
        """

        if product_cluster == 'not_tariff':
            folder_download = f"""{reporter_name}_{type_flow}_{qty_or_value}"""
        else:
            folder_download = f"""{reporter_name}_{type_flow}_{qty_or_value}_Tariff"""
        if not os.path.exists(folder_download):
            os.mkdir(folder_download)
        full_path_download = os.path.join(os.getcwd(), folder_download)

        options = webdriver.ChromeOptions()

        # Для отключения визуального интерфейса (окна браузера)
        # options.add_argument("--headless=new")
        # options.add_argument("--disable-gpu")

        # Отключаем настройку, которая сообщает, что брауезр управляется автоматически
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # Отключает использование расширений автоматизации, чтобы скрыть, что брауезр управляется автоматически
        options.add_experimental_option("useAutomationExtension", False)
        # Для того чтобы сайт думал, что перед ним реальный браузер
        options.add_argument("--disable-blink-features=AutomationControlled")
        # Свой user-agent, чтобы сделать браузе похожий на свой
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
        options.add_argument('--proxy-server=%s' % self.proxy)

        # Настройки для указания папки загрузки файлов и отключение окна запроса на сохранение логина и пароля
        prefs = {
            "download.default_directory": full_path_download,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        options.add_experimental_option("prefs", prefs)

        # ДЛЯ ПРОВЕРКИ, ЧТО работаем, через прокси
        # with webdriver.Chrome(options=options) as browser:
        #     browser.get('https://2ip.ru/')
        #     time.sleep(35)

        with webdriver.Chrome(options=options) as browser:
            if qty_or_value == 'Values' and product_cluster == 'not_tariff':
                self.downloading_trade_value(browser, type_flow, reporter_name, full_path_download,
                                             flag_insert_user=flag_insert_user)
            elif qty_or_value == 'Quantities' and product_cluster == 'not_tariff':
                self.downloading_quantities(browser, type_flow, reporter_name, full_path_download,
                                            partner_list=partner_list, flag_insert_user=flag_insert_user)
            elif qty_or_value == 'Values' and product_cluster == 'tariff':
                self.downloading_tariff_line_value(browser, type_flow, reporter_name, full_path_download,
                                                   flag_insert_user=flag_insert_user)
