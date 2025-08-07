from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """
    Управление всеми настройками софта
    """

    # Общие
    PROJECT_NAME: str = "Product Matcher"  # Название проекта
    PROJECT_DESC: str = 'Сервис для подбора товаров из бд товарам из тендера. (ElasticSearch для поиска, БД с нормализованными характеристиками)'
    PROJECT_VERSION: str = '1.0.0'
    SOFTWARE_RESTART_TIME: int = 15  # Время (в СЕКУНДАХ) автоматического перезапуска софта после падения

    # Настройка логирования
    LOG_LEVEL: str = "DEBUG"  # Доступные уровни логирования - DEBUG, INFO, WARNING, ERROR, FATAL
    LOG_FORMAT: str = '%(asctime).19s | %(levelname).3s | %(message)s'  # Формат отображения логов

    # Настройка API (fastapi)
    API_APP: str = "src.main:app"
    API_HOST: str = 'localhost'
    API_PORT: int = 8080
    API_LOG_LEVEL: str = "info"

    # Настройка подключения к Elasticsearch
    ELASTICSEARCH_HOST: str = 'localhost'
    ELASTICSEARCH_PORT: str = '9200'
    ELASTICSEARCH_INDEX: str = 'products'
    ES_HOSTS: str = "http://localhost:9200"
    ES_REQUEST_TIMEOUT: int = 30
    ES_MAX_RETRIES: int = 3
    ES_RETRY_ON_TIMEOUT: bool = True
    ES_SNIFF_ON_START: bool = True
    ES_SNIFF_ON_CONNECTION_FAIL: bool = True
    ES_SNIFFER_TIMEOUT: int = 60

    # Настройка подключения к MongoDB
    MONGO_HOST: str = 'localhost'
    MONGO_PORT: int = 27017
    MONGO_DB_NAME: str = 'local'
    MONGO_COLLECTION_NAME_DATASET: str = 'diversified_products'
    MONGO_COLLECTION_NAME_CATEGORIES: str = 'yandex_categories'
    MONGO_COLLECTION_NAME_ATTRIBUTES: str = 'yandex_attributes'

    # Получение ссылки для подключения к MongoDB
    @property
    def get_mongo_connection_link(self):
        return f'mongodb://{self.MONGO_HOST}:{self.MONGO_PORT}/{self.MONGO_DB_NAME}'

    # Настройка подключения к Postgres
    PG_HOST: str = 'localhost'
    PG_USER: str = 'postgres'
    PG_PASS: str = 'postgres'
    PG_PORT: int = 5432
    PG_DB_NAME: str = 'vector'

    # Получение ссылки для подключения к Postgres
    @property
    def get_postgres_connection_link(self):
        return f'postgresql+asyncpg://{self.PG_USER}:{self.PG_PASS}@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DB_NAME}'


    class Config:
        env_file = ".env_testik"
        case_sensitive = False


settings = Settings()
