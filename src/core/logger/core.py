import inspect
import logging
import os


class ContextLogger:
    def __init__(self, format: str, project_name: str, level: int):
        self.logger = self.setup_logger(format=format, logger_name=project_name, level=level)

    @staticmethod
    def setup_logger(
            format: str = '%(asctime).19s | %(levelname).3s | %(message)s',
            logger_name: str = 'base_logger',
            level: int = logging.DEBUG
        ) -> logging.Logger:
        """Настройка логера"""

        # Настраиваем basicConfig только один раз для корневого логера
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=level,
                format=format,
            )
        logger = logging.getLogger(name=logger_name)

        logger.setLevel(level)

        return logger

    def debug(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        self.logger.debug(f"{module_name} | {function_name} | {message}")

    def info(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        self.logger.info(f"{module_name} | {function_name} | {message}")

    def warning(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        self.logger.warning(f"{module_name} | {function_name} | {message}")

    def error(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        self.logger.error(f"{module_name} | {function_name} | {message}")

    def critical(self, message):
        frame = inspect.currentframe().f_back
        function_name = f'{inspect.currentframe().f_back.f_code.co_name}'
        module_name = os.path.basename(inspect.getmodule(frame).__file__)

        self.logger.critical(f"{module_name} | {function_name} | {message}")
