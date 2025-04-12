# домашняя работа OTUS Spark

Для проверки нужно запустить команду:
```
docker-compose up -d
```

Затем запустить команду
```
docker exec -it spark-jupyter /usr/local/spark/bin/spark-submit --master spark://spark-master:7077 ./work/homework.py src_dir trg_dir
```
где
src_dir - путь в исходным файлам
trg_dir - путь к исходящему файлу витрины
