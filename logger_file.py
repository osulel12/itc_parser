import logging


class logger:
    def __init__(self, name):
        self.name = name

    def create_logger(self):
        py_logger = logging.getLogger(self.name)
        py_logger.setLevel(logging.INFO)

        # настройка обработчика и форматировщика в соответствии с нашими нуждами
        py_handler = logging.FileHandler(f"{self.name}.txt", mode='w', encoding='utf-8')
        py_formatter = logging.Formatter("%(filename)s %(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M')

        # добавление форматировщика к обработчику
        py_handler.setFormatter(py_formatter)
        # добавление обработчика к логгеру
        py_logger.addHandler(py_handler)
        return py_logger
