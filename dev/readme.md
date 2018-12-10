### Локальный запуск Kafka + Zookeeper

builds from [docker hub](https://hub.docker.com/u/confluent/)

```docker-compose up```

### Передача собщении в kafka из файла

* Создать окружение dev/requirements.txt 
    * ```virtualenv -p python3 venv```
    * ```source venv/bic/activate ```
* Запустить скрипт для передачи сообщений из файла
    * ```pip install -r dev/requirements.txt```
    * ```python kafka_producer_from_file.py --file path/to/input_file.txt --topic sensor```
    
### Чтение сообщений из kafka (для проверки работоспособности)
* ```python kafka_consumer.py --topic sensor```


#### Elasticsearch

* удалить индекс ```curl -X DELETE "localhost:9200/sensor"```

