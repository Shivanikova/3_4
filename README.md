# 3_4
### Текст задания размещен в файле 'Задание.txt'

### Для подключения к контейнеру папку airflow необходимо перенести на локальный компьютер. В командной строке нужно войти в эту папку и набрать команду docker-compose up.
### Для сборки образа используется файл 'docker_compose.yml', размещенный на странице курса и перенесенный в папку airflow.
#### В появившейся папке airflow/airflow/dags необходимо разместить файл calc_rates2.py.
#### веб-сервер airflow будет доступен по ссылке:
http:/localhost:8080 с базовыми параметрами логина airflow и пароля airflow.

####  При создании volumes нижеуказанный Волюм закомментирован, таблица rates_BTC_new в постгрес создавалась по результату отработки DAGa calc_rates2.py
volumes:
     
      - ./postgres/init_scripts/task1.sql:/docker-entrypoint-initdb.d/init.sql

#### Получившиеся результаты размещены в папке 'Результаты работы DAGa'

##Настройки подключения my_db_conn в Airflow:

####connection_id = 'my_db_conn'
####connection Type = 'Postgres'
####host = 'host.docker.internal'
####schema = 'test'
####login = 'postgres'
####password = 'password'
####port = '5430'

##Настройки подключения в DBeaver

####хост - 'localhost'
####База данных - 'test'
####Пользователь - 'postgres'
####Пароль - 'password'

##Cодержание переменной variables

####variables = {"table_name": "rates_BTC_new",
####                                "RUB": "RUB",
####                                "USD": "USD",                              
####                                "BTC": "BTC", 
####                                "EUR": "EUR",
####                                "connection_name":"my_db_conn",
####                                "url_base":"https://api.exchangerate.host/"}

