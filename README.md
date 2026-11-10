# Лабораторная работа 4 по инженерии данных

Отказоустойчивость средствами Apache ZooKeeper. Вариант 9, программист, разработчик на мобильных платформах.

Отчет лежит в [output/Лабораторная_работа_4_ZooKeeper.pdf](output/Лабораторная_работа_4_ZooKeeper.pdf).

## Что сделано

Ансамбль из трех серверов ZooKeeper 3.9.6 с Java 17 поднят в Docker. У каждого сервера своя конфигурация conf/zkN/zoo.cfg, свой клиентский порт 2181, 2182 или 2183 и своя папка dataDir с файлом myid.

Проверены роли серверов, работа без одного сервера, остановка без двух серверов и эфемерный узел в zkCli.

Координатор scripts/coordinator.py считает пары навыков варианта из третьей работы по объекту s3://hh-lake/raw/it_vacancies_2024.csv в RustFS. Лидер выбирается через эфемерные последовательные узлы, номер эпохи хранится в /lab4/epoch, готовые сплиты в /lab4/checkpoint, указатель на результат в /lab4/result. Таймаут сессии 10 секунд.

Скрипт scripts/scenario.py запускает две копии координатора, приостанавливает лидера посреди расчета на 40 секунд и проверяет итог. Запуск без защиты дает две записи результата и указатель версии 2. Запуск с FENCING=1 дает отклонение записи старого лидера с BadVersionError. В обоих запусках результат совпадает с файлом пар из третьей работы по SHA256.

## Запуск

Нужны Docker, Python 3.12 и RustFS из третьей работы с объектом hh-lake/raw/it_vacancies_2024.csv. В файле .env задаются RUSTFS_ACCESS_KEY и RUSTFS_SECRET_KEY.

```sh
python3 -m venv .venv
.venv/bin/pip install kazoo boto3 playwright
./scripts/ensemble.sh
.venv/bin/python scripts/scenario.py stage3_nofence 0
.venv/bin/python scripts/scenario.py stage4_fence 1
```

## Файлы

compose.yaml и conf описывают три сервера, conf/logback-cli.xml оставляет в выводе zkCli только результаты команд и ошибки, data/zkN/myid задают номера серверов.

scripts/ensemble.sh выполняет этапы 1 и 2 и пишет выводы команд в logs. scripts/common.py содержит подключение к RustFS, условие варианта и разбор навыков из третьей работы.

В logs лежат выводы команд этапов 1 и 2, а в logs/stage3_nofence и logs/stage4_fence журналы координаторов coord-a и coord-b и итог сценария. В screenshots снимки для отчета, в report исходник отчета в LaTeX.
