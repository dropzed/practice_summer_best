#!/usr/bin/env bash

# Цвета для вывода в консоль
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # Без цвета

echo -e "${CYAN}=====================================================${NC}"
echo -e "${CYAN}      Верификация распределенного брокера сообщений    ${NC}"
echo -e "${CYAN}=====================================================${NC}"

# Функция очистки фоновых процессов при выходе
cleanup() {
    echo -e "\n${YELLOW}[*] Очистка фоновых процессов и временных папок...${NC}"
    pkill -f "./bin/manager" || true
    pkill -f "./bin/broker" || true
    pkill -f "./bin/demo" || true
    sleep 1
}
trap cleanup EXIT

# Завершаем старые процессы от предыдущих запусков
echo -e "${YELLOW}[*] Остановка запущенных ранее процессов broker/manager/demo...${NC}"
pkill -f "bin/manager" || true
pkill -f "bin/broker" || true
pkill -f "bin/demo" || true
sleep 1

# 1. Очистка старых данных
echo -e "${YELLOW}[*] Сброс директорий с логами...${NC}"
rm -rf data/
mkdir -p data/manager data/broker logs

# 2. Сборка проекта
echo -e "${YELLOW}[*] Сборка бинарных файлов...${NC}"
../.go/bin/go build -o bin/manager cmd/manager/main.go || exit 1
../.go/bin/go build -o bin/broker cmd/broker/main.go || exit 1
../.go/bin/go build -o bin/demo cmd/demo/main.go || exit 1

# 3. Запуск Queue Manager (МО)
echo -e "${YELLOW}[*] Запуск Queue Manager (МО) на порту 8080...${NC}"
./bin/manager -port 8080 -state data/manager/state.json > logs/manager.log 2>&1 &
QM_PID=$!
sleep 1

# 4. Запуск Service Broker (СБ)
echo -e "${YELLOW}[*] Запуск Service Broker (СБ) на порту 8081...${NC}"
./bin/broker -id broker-1 -port 8081 -qm http://localhost:8080 -data data/broker > logs/broker-1.log 2>&1 &
BROKER_PID=$!
sleep 2 # Ждем регистрации брокера и отправки хартбита

# =====================================================================
# Сценарий 1: БЕЗОПАСНОСТЬ ДАННЫХ (DATA SAFETY)
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Сценарий 1: Безопасность данных (Append-only + Fsync + Восстановление) ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Публикация 1000 сообщений в топик 'safety-topic'...${NC}"

# Публикуем 1000 сообщений
./bin/demo -mode publish -topic safety-topic -count 1000 -payload "safety-msg" > logs/publish_safety.log 2>&1

echo -e "${YELLOW}[*] Убиваем процесс брокера (kill -9) для симуляции сбоя...${NC}"
kill -9 $BROKER_PID
sleep 2

echo -e "${YELLOW}[*] Перезапуск Service Broker...${NC}"
./bin/broker -id broker-1 -port 8081 -qm http://localhost:8080 -data data/broker > logs/broker-1-restart.log 2>&1 &
BROKER_PID=$!
sleep 2 # Даем брокеру время перезагрузить логи и зарегистрироваться

echo -e "${YELLOW}[*] Подписчик считывает 1000 сообщений из перезапущенного брокера...${NC}"
./bin/demo -mode subscribe -topic safety-topic -group safety-group -id sub-safety -count 1000 > logs/subscribe_safety.log 2>&1

# Проверка количества прочитанных сообщений
CONSUMED_COUNT=$(grep -c "Received: offset=" logs/subscribe_safety.log || true)
if [ "$CONSUMED_COUNT" -eq 1000 ]; then
    echo -e "${GREEN}[SUCCESS] Безопасность данных: Успешно восстановлено и прочитано 1000 сообщений!${NC}"
else
    echo -e "${RED}[FAILURE] Безопасность данных: Ожидалось 1000 сообщений, прочитано $CONSUMED_COUNT. Проверьте logs/subscribe_safety.log${NC}"
fi

# =====================================================================
# Сценарий 2: БАЛАНСИРОВКА НАГРУЗКИ (LOAD BALANCING)
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Сценарий 2: Балансировка нагрузки в группе потребителей ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Запуск 2 экземпляров подписчиков в группе 'lb-group'...${NC}"

# Запускаем подписчиков A и B в фоне
./bin/demo -mode subscribe -topic lb-topic -group lb-group -id sub-A -count 5 -delay 200 -limit 1 > logs/sub_A.log 2>&1 &
SUB_A_PID=$!
./bin/demo -mode subscribe -topic lb-topic -group lb-group -id sub-B -count 5 -delay 200 -limit 1 > logs/sub_B.log 2>&1 &
SUB_B_PID=$!

sleep 1

echo -e "${YELLOW}[*] Публикация 10 сообщений в топик 'lb-topic'...${NC}"
./bin/demo -mode publish -topic lb-topic -count 10 -payload "lb-msg" > logs/publish_lb.log 2>&1

# Ждем завершения работы подписчиков
sleep 4

# Проверяем, сколько сообщений обработал каждый подписчик
COUNT_A=$(grep -c "Received: offset=" logs/sub_A.log || true)
COUNT_B=$(grep -c "Received: offset=" logs/sub_B.log || true)

echo -e "Подписчик A обработал: ${CYAN}$COUNT_A${NC} сообщений"
echo -e "Подписчик B обработал: ${CYAN}$COUNT_B${NC} сообщений"

if [ "$COUNT_A" -gt 0 ] && [ "$COUNT_B" -gt 0 ] && [ $((COUNT_A + COUNT_B)) -eq 10 ]; then
    echo -e "${GREEN}[SUCCESS] Балансировка: Сообщения распределены между подписчиками. Всего обработано: 10/10.${NC}"
else
    echo -e "${RED}[FAILURE] Балансировка: Распределение не удалось или сообщения потеряны. Всего обработано: $((COUNT_A + COUNT_B))/10.${NC}"
fi

# =====================================================================
# Сценарий 3: ВОЗОБНОВЛЕНИЕ С ЦЕЛЕВОГО СМЕЩЕНИЯ (RESTART FROM OFFSET)
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Сценарий 3: Возобновление работы со смещения (Restart from Offset) ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Публикация 5 сообщений в топик 'restart-topic'...${NC}"
./bin/demo -mode publish -topic restart-topic -count 5 -payload "restart-msg-part1" > logs/publish_restart_1.log 2>&1

echo -e "${YELLOW}[*] Запуск Подписчика A для чтения 5 первых сообщений и остановки...${NC}"
./bin/demo -mode subscribe -topic restart-topic -group restart-group -id sub-restart -count 5 > logs/sub_restart_1.log 2>&1

echo -e "${YELLOW}[*] Публикация еще 5 сообщений в 'restart-topic', пока Подписчик оффлайн...${NC}"
./bin/demo -mode publish -topic restart-topic -count 5 -payload "restart-msg-part2" > logs/publish_restart_2.log 2>&1

echo -e "${YELLOW}[*] Перезапуск Подписчика A для чтения оставшихся сообщений...${NC}"
./bin/demo -mode subscribe -topic restart-topic -group restart-group -id sub-restart -count 5 > logs/sub_restart_2.log 2>&1

# Проверяем, что второй запуск начался со смещения 6
FIRST_OFFSET=$(grep -o "offset=[0-9]*" logs/sub_restart_2.log | head -n 1 | cut -d= -f2 || true)
LAST_OFFSET=$(grep -o "offset=[0-9]*" logs/sub_restart_2.log | tail -n 1 | cut -d= -f2 || true)
RESUMED_COUNT=$(grep -c "Received: offset=" logs/sub_restart_2.log || true)

echo -e "Перезапущенный подписчик прочитал: ${CYAN}$RESUMED_COUNT${NC} сообщений"
echo -e "Смещения во втором запуске: ${CYAN}с $FIRST_OFFSET по $LAST_OFFSET${NC}"

if [ "$FIRST_OFFSET" -eq 6 ] && [ "$LAST_OFFSET" -eq 10 ] && [ "$RESUMED_COUNT" -eq 5 ]; then
    echo -e "${GREEN}[SUCCESS] Возобновление работы: Подписчик корректно продолжил работу со смещения 6.${NC}"
else
    echo -e "${RED}[FAILURE] Возобновление работы: Сбой или неверные смещения: FirstOffset=$FIRST_OFFSET, LastOffset=$LAST_OFFSET.${NC}"
fi

# =====================================================================
# Сценарий 4: МНОЖЕСТВЕННЫЕ ГРУППЫ (MULTIPLE GROUPS)
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Сценарий 4: Множественные группы (независимая доставка) ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Публикация 5 сообщений в топик 'multigroup-topic'...${NC}"
./bin/demo -mode publish -topic multigroup-topic -count 5 -payload "multi-msg" > logs/publish_multigroup.log 2>&1

echo -e "${YELLOW}[*] Запуск Группы Подписчиков A...${NC}"
./bin/demo -mode subscribe -topic multigroup-topic -group group-A -id sub-groupA -count 5 > logs/sub_groupA.log 2>&1

echo -e "${YELLOW}[*] Запуск Группы Подписчиков B...${NC}"
./bin/demo -mode subscribe -topic multigroup-topic -group group-B -id sub-groupB -count 5 > logs/sub_groupB.log 2>&1

COUNT_A_MG=$(grep -c "Received: offset=" logs/sub_groupA.log || true)
COUNT_B_MG=$(grep -c "Received: offset=" logs/sub_groupB.log || true)

echo -e "Группа A обработала: ${CYAN}$COUNT_A_MG${NC} сообщений"
echo -e "Группа B обработала: ${CYAN}$COUNT_B_MG${NC} сообщений"

if [ "$COUNT_A_MG" -eq 5 ] && [ "$COUNT_B_MG" -eq 5 ]; then
    echo -e "${GREEN}[SUCCESS] Множественные группы: Обе группы независимо получили все 5 сообщений.${NC}"
else
    echo -e "${RED}[FAILURE] Множественные группы: Ошибка независимой доставки. Группа A: $COUNT_A_MG, Группа B: $COUNT_B_MG.${NC}"
fi

# =====================================================================
# Сценарий 5: МУЛЬТИБРОКЕРНОСТЬ И API СТАТУСА
# =====================================================================
echo -e "\n${CYAN}-----------------------------------------------------${NC}"
echo -e "${CYAN} Сценарий 5: Мультиброкерная маршрутизация и API статуса ${NC}"
echo -e "${CYAN}-----------------------------------------------------${NC}"
echo -e "${YELLOW}[*] Запуск второго инстанса брокера 'broker-2' на порту 8082...${NC}"
./bin/broker -id broker-2 -port 8082 -qm http://localhost:8080 -data data/broker > logs/broker-2.log 2>&1 &
BROKER_2_PID=$!
sleep 2

echo -e "${YELLOW}[*] Публикация в новый топик 'topic-broker2'...${NC}"
./bin/demo -mode publish -topic topic-broker2 -count 3 -payload "broker2-msg" > logs/publish_broker2.log 2>&1

echo -e "${YELLOW}[*] Запрос эндпоинта /status у Queue Manager (МО):${NC}"
./bin/demo -mode status

echo -e "\n${CYAN}=====================================================${NC}"
echo -e "${CYAN}           Процесс проверки завершен                 ${NC}"
echo -e "${CYAN}=====================================================${NC}"
