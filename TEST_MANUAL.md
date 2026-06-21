РУКОВОДСТВО ПО РУЧНОМУ ТЕСТИРОВАНИЮ (SENIOR ВЕРСИЯ)

Это руководство описывает шаги по ручному тестированию высокопроизводительного и отказоустойчивого брокера сообщений.

---------------------------------------------------------
1. СБОРКА КОМПОНЕНТОВ
---------------------------------------------------------

Выполните сборку в корне папки senior:
mkdir -p bin
../.go/bin/go build -o bin/manager cmd/manager/main.go
../.go/bin/go build -o bin/broker cmd/broker/main.go
../.go/bin/go build -o bin/demo cmd/demo/main.go

---------------------------------------------------------
2. ЗАПУСК ИНФРАСТРУКТУРЫ
---------------------------------------------------------

Откройте три разных терминала:

* Терминал 1 (Queue Manager):
./bin/manager -port 8080 -state data/manager/state.json

* Терминал 2 (Broker 1):
./bin/broker -id broker-1 -port 8081 -qm http://localhost:8080 -data data/broker

* Терминал 3 (Broker 2):
./bin/broker -id broker-2 -port 8082 -qm http://localhost:8080 -data data/broker

---------------------------------------------------------
3. РУЧНОЕ ВЫПОЛНЕНИЕ СЦЕНАРИЕВ ТЕСТИРОВАНИЯ
---------------------------------------------------------

Кейс 1: Data Safety (Сохранение данных и восстановление индекса)
1. Опубликуйте 10 сообщений:
./bin/demo -mode publish -topic safety-test -count 10 -payload "data"

2. Убейте Брокер 1 с помощью kill -9.
3. Перезапустите Брокер 1 (Терминал 2). Он мгновенно восстановит индекс в памяти за один проход по файлу.
4. Вычитайте сообщения:
./bin/demo -mode subscribe -topic safety-test -group group-safety -id sub-1 -count 10
Все 10 сообщений будут прочитаны из Durability Log.

Кейс 2: Load Balancing (Равномерное распределение)
1. Запустите двух подписчиков в разных терминалах:
Терминал А:
./bin/demo -mode subscribe -topic lb-test -group group-lb -id sub-A -count 5 -delay 200 -limit 1
Терминал Б:
./bin/demo -mode subscribe -topic lb-test -group group-lb -id sub-B -count 5 -delay 200 -limit 1

2. Опубликуйте 10 сообщений:
./bin/demo -mode publish -topic lb-test -count 10 -payload "msg"

Каждый подписчик обработает строго по 5 сообщений без дублирования благодаря механизму аренды.

Кейс 3: Restart from Offset (Возобновление с правильного места)
1. Опубликуйте 5 сообщений:
./bin/demo -mode publish -topic restart-test -count 5 -payload "msg"

2. Прочитайте их и завершите сессию:
./bin/demo -mode subscribe -topic restart-test -group group-r -id sub-x -count 5

3. Отправьте еще 5 сообщений (офсеты 6-10).
4. Запустите подписчика повторно:
./bin/demo -mode subscribe -topic restart-test -group group-r -id sub-x -count 5
Чтение начнется строго с офсета 6.

Кейс 4: Multiple Groups (Независимая доставка)
1. Опубликуйте 5 сообщений:
./bin/demo -mode publish -topic multi-test -count 5 -payload "msg"

2. Запустите подписчика Группы А:
./bin/demo -mode subscribe -topic multi-test -group group-A -id sub-A -count 5

3. Запустите подписчика Группы Б:
./bin/demo -mode subscribe -topic multi-test -group group-B -id sub-B -count 5
Обе группы получат все 5 сообщений независимо друг от друга.

---------------------------------------------------------
4. МОНИТОРИНГ
---------------------------------------------------------

Для просмотра статуса кластера (активные брокеры, топики, группы, офсеты и количество невыданных/арендованных сообщений) выполните:
./bin/demo -mode status
