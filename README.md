# Product Matcher
#### Сервис для подбора товаров из бд товарам из тендера. (ElasticSearch для поиска, БД с нормализованными характеристиками)

---

---

## Как пользоваться API Swagger
- GET /api/v1/healthz - Проверка здоровья сервиса для Кубернетис;
- GET /api/v1/search/search_es - Поиск товара по строгому синтаксису;
- GET /api/v1/search/search_es_fuzzy - Поиск товара по нестрогому синтаксису;
- DELETE /api/v1/data/delete_all_indexes - Удалить все индексы из ElasticSearch;


## Основные слои репозитория
- api
- services
- repository

Настройка основных функций софта находится в product_matcher/src/config/config, настройки подтягиваются из `.env`.
