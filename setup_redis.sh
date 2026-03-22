#!/usr/bin/env bash
# ============================================================================
#  Redis Setup & Health Check Script
#  Crypto Spread Monitor — Unix Socket, No Persistence, Low Latency
# ============================================================================
#
#  Использование:
#    sudo bash setup_redis.sh          — полная установка + настройка + проверка
#    sudo bash setup_redis.sh --check  — только проверка (без изменений)
#
# ============================================================================

set -uo pipefail

# ========================== КОНФИГУРАЦИЯ ====================================

REDIS_CONF="/etc/redis/redis.conf"
REDIS_CONF_BACKUP="/etc/redis/redis.conf.bak.$(date +%Y%m%d_%H%M%S)"
REDIS_SOCK="/var/run/redis/redis.sock"
REDIS_SOCK_DIR="/var/run/redis"
REDIS_USER="redis"
REDIS_GROUP="redis"
APP_USER="${APP_USER:-$(logname 2>/dev/null || echo "${SUDO_USER:-root}")}"
MAXMEMORY="40gb"
SYSCTL_CONF="/etc/sysctl.d/99-redis.conf"
THP_SERVICE="/etc/systemd/system/disable-thp.service"
LIMITS_CONF="/etc/security/limits.d/redis.conf"
REDIS_MAJOR=0
REDIS_VERSION="unknown"

# ========================== ЦВЕТА ==========================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass=0
fail=0
warn_count=0

ok()      { echo -e "  ${GREEN}✓${NC} $1"; pass=$((pass + 1)); }
fail()    { echo -e "  ${RED}✗${NC} $1"; fail=$((fail + 1)); }
warning() { echo -e "  ${YELLOW}⚠${NC} $1"; warn_count=$((warn_count + 1)); }
info()    { echo -e "  ${BLUE}ℹ${NC} $1"; }
header()  { echo -e "\n${BOLD}${CYAN}━━━ $1 ━━━${NC}\n"; }

# ========================== ПРОВЕРКА ROOT ==================================

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Запусти от root: sudo bash $0${NC}"
    exit 1
fi

# ========================== РЕЖИМ ==========================================

CHECK_ONLY=false
if [[ "${1:-}" == "--check" ]]; then
    CHECK_ONLY=true
    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}${CYAN}║     Redis Health Check (read-only)       ║${NC}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════╝${NC}"
else
    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}${CYAN}║     Redis Setup & Configuration          ║${NC}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════╝${NC}"
fi

echo ""
echo -e "  Дата:         $(date '+%Y-%m-%d %H:%M:%S')"
echo -e "  Пользователь: ${APP_USER}"
echo -e "  Сокет:        ${REDIS_SOCK}"

# ============================================================================
#  ЭТАП 1: УСТАНОВКА REDIS
# ============================================================================

if [[ "$CHECK_ONLY" == false ]]; then

header "1/6  Установка Redis"

if command -v redis-server &>/dev/null; then
    REDIS_VERSION=$(redis-server --version | grep -oP 'v=\K[0-9.]+' || echo "0.0.0")
    REDIS_MAJOR=$(echo "$REDIS_VERSION" | cut -d. -f1)
    ok "Redis установлен: v${REDIS_VERSION}"

    if [[ "$REDIS_MAJOR" -lt 7 ]]; then
        warning "Redis ${REDIS_VERSION} — версия < 7. io-threads и часть фич будут пропущены"
    else
        ok "Redis 7+ — все оптимизации доступны"
    fi
else
    info "Устанавливаю Redis..."
    if ! apt-get update -qq; then
        fail "apt-get update не удался"
    fi
    if ! apt-get install -y -qq redis-server >/dev/null 2>&1; then
        fail "Не удалось установить Redis"
        echo -e "${RED}Проверь apt-get install redis-server вручную${NC}"
        exit 1
    fi
    REDIS_VERSION=$(redis-server --version | grep -oP 'v=\K[0-9.]+' || echo "0.0.0")
    REDIS_MAJOR=$(echo "$REDIS_VERSION" | cut -d. -f1)
    ok "Redis установлен: v${REDIS_VERSION}"
fi

# ============================================================================
#  ЭТАП 2: НАСТРОЙКА LINUX
# ============================================================================

header "2/6  Настройка ОС (sysctl, THP, limits)"

# --- Transparent Huge Pages ---
info "Отключаю Transparent Huge Pages..."

cat > "$THP_SERVICE" << 'THPEOF'
[Unit]
Description=Disable Transparent Huge Pages (for Redis)
DefaultDependencies=no
After=sysinit.target local-fs.target
Before=redis-server.service

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled && echo never > /sys/kernel/mm/transparent_hugepage/defrag'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
THPEOF

systemctl daemon-reload
if systemctl enable --now disable-thp.service >/dev/null 2>&1; then
    ok "THP systemd unit создан и активирован"
else
    echo never > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
    echo never > /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null || true
    ok "THP отключены напрямую (systemd unit не сработал)"
fi

# --- sysctl ---
info "Настраиваю sysctl..."

cat > "$SYSCTL_CONF" << EOF
# Redis optimizations — generated $(date '+%Y-%m-%d')
vm.overcommit_memory = 1
net.core.somaxconn = 1024
net.ipv4.tcp_max_syn_backlog = 1024
EOF

sysctl -p "$SYSCTL_CONF" >/dev/null 2>&1 || warning "sysctl -p вернул ошибку (не критично)"
ok "sysctl: overcommit=1, somaxconn=1024"

# --- limits ---
cat > "$LIMITS_CONF" << EOF
${REDIS_USER} soft nofile 65536
${REDIS_USER} hard nofile 65536
EOF
ok "Лимиты fd: 65536"

# ============================================================================
#  ЭТАП 3: НАСТРОЙКА redis.conf
# ============================================================================

header "3/6  Конфигурация redis.conf"

if [[ -f "$REDIS_CONF" ]]; then
    cp "$REDIS_CONF" "$REDIS_CONF_BACKUP"
    ok "Бэкап: ${REDIS_CONF_BACKUP}"
fi

# Общая часть (Redis 6+)
cat > "$REDIS_CONF" << EOF
# ============================================================================
# Redis Configuration — Crypto Spread Monitor
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# Redis: v${REDIS_VERSION}
# ============================================================================

# --- Сеть: только Unix socket, TCP выключен ---
port 0
unixsocket ${REDIS_SOCK}
unixsocketperm 770
timeout 0
tcp-keepalive 300

# --- Память ---
maxmemory ${MAXMEMORY}
maxmemory-policy volatile-ttl

# --- Persistence: ВЫКЛЮЧЕНА (данные volatile) ---
save ""
appendonly no

# --- Производительность ---
hz 100
dynamic-hz yes
lazyfree-lazy-eviction yes
lazyfree-lazy-expire yes
lazyfree-lazy-server-del yes

# --- Логирование ---
loglevel notice
logfile /var/log/redis/redis-server.log

# --- Мониторинг задержек ---
latency-monitor-threshold 5
slowlog-log-slower-than 1000
slowlog-max-len 1024

# --- Pub/Sub защита буфера ---
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit replica 256mb 64mb 60
client-output-buffer-limit pubsub 8mb 4mb 60

# --- Безопасность: блокировка опасных команд ---
rename-command KEYS ""
rename-command FLUSHALL ""
rename-command FLUSHDB ""
rename-command DEBUG ""

# --- Systemd integration ---
daemonize no
supervised systemd

# --- Прочее ---
databases 2
stop-writes-on-bgsave-error no
EOF

# Redis 7+ features
if [[ "$REDIS_MAJOR" -ge 7 ]]; then
    cat >> "$REDIS_CONF" << EOF

# --- Redis 7+ features ---
io-threads 8
io-threads-do-reads yes
lazyfree-lazy-user-del yes
activedefrag yes
active-defrag-ignore-bytes 10mb
EOF
    ok "redis.conf сгенерирован (Redis 7+ режим)"
else
    cat >> "$REDIS_CONF" << EOF

# --- Redis 6 mode ---
# io-threads пропущены (Redis 7+)
# activedefrag пропущен (нестабилен в Redis 6)
EOF
    ok "redis.conf сгенерирован (Redis 6 режим)"
fi

# ============================================================================
#  ЭТАП 4: ПРАВА И ДИРЕКТОРИИ
# ============================================================================

header "4/6  Права доступа"

mkdir -p "$REDIS_SOCK_DIR"
chown "${REDIS_USER}:${REDIS_GROUP}" "$REDIS_SOCK_DIR"
chmod 750 "$REDIS_SOCK_DIR"
ok "Директория сокета: ${REDIS_SOCK_DIR}"

mkdir -p /var/log/redis
chown "${REDIS_USER}:${REDIS_GROUP}" /var/log/redis
ok "Директория логов: /var/log/redis"

if id "$APP_USER" &>/dev/null; then
    if groups "$APP_USER" 2>/dev/null | grep -qw "$REDIS_GROUP"; then
        ok "${APP_USER} уже в группе ${REDIS_GROUP}"
    else
        usermod -aG "$REDIS_GROUP" "$APP_USER"
        ok "${APP_USER} добавлен в группу ${REDIS_GROUP}"
        warning "Перелогинься или выполни: newgrp redis"
    fi
else
    warning "Пользователь ${APP_USER} не найден — пропускаю"
fi

# ============================================================================
#  ЭТАП 5: ПЕРЕЗАПУСК
# ============================================================================

header "5/6  Перезапуск Redis"

# Определить имя systemd-сервиса
REDIS_SERVICE=""
for svc in redis-server redis; do
    if systemctl list-unit-files "${svc}.service" >/dev/null 2>&1; then
        REDIS_SERVICE="$svc"
        break
    fi
done

if [[ -z "$REDIS_SERVICE" ]]; then
    fail "Не нашёл systemd unit для Redis"
    info "Попробуй: systemctl list-units | grep redis"
    exit 1
fi

info "Сервис: ${REDIS_SERVICE}"

# Создать systemd override: Type=notify + увеличенный таймаут
OVERRIDE_DIR="/etc/systemd/system/${REDIS_SERVICE}.service.d"
mkdir -p "$OVERRIDE_DIR"
cat > "${OVERRIDE_DIR}/override.conf" << EOF
[Service]
Type=notify
TimeoutStartSec=30
TimeoutStopSec=15
LimitNOFILE=65536
EOF
ok "Systemd override: Type=notify, Timeout=30s"

systemctl daemon-reload
sleep 1

# Остановить, подождать, стартовать (чище чем restart)
systemctl stop "$REDIS_SERVICE" 2>/dev/null || true
sleep 1
systemctl start "$REDIS_SERVICE"
sleep 2

if systemctl is-active --quiet "$REDIS_SERVICE"; then
    ok "Redis запущен"
else
    fail "Redis НЕ запустился!"
    echo ""
    echo -e "  ${RED}Последние строки лога:${NC}"
    journalctl -u "$REDIS_SERVICE" -n 15 --no-pager 2>/dev/null || true
    echo ""
    echo -e "  ${RED}Попробуй вручную:${NC}"
    echo -e "  redis-server ${REDIS_CONF} --daemonize no"
    exit 1
fi

# Подождём сокет
for _i in 1 2 3 4 5; do
    [[ -S "$REDIS_SOCK" ]] && break
    sleep 1
done

if [[ -S "$REDIS_SOCK" ]]; then
    ok "Сокет доступен: ${REDIS_SOCK}"
else
    fail "Сокет не появился через 5 секунд"
    info "Проверь: ls -la ${REDIS_SOCK_DIR}/"
fi

fi  # конец CHECK_ONLY == false

# ============================================================================
#  ЭТАП 6: ПОЛНАЯ ПРОВЕРКА ЗДОРОВЬЯ
# ============================================================================

header "6/6  Health Check"

# CLI: сокет или TCP фоллбэк
if [[ -S "$REDIS_SOCK" ]]; then
    CLI="redis-cli -s ${REDIS_SOCK}"
else
    CLI="redis-cli"
    warning "Сокет не найден, пробую TCP localhost"
fi

# --- 6.1 Доступность ---
echo -e "${BOLD}  Доступность${NC}"

if [[ -S "$REDIS_SOCK" ]]; then
    ok "Сокет существует: ${REDIS_SOCK}"
    SOCK_PERMS=$(stat -c '%a' "$REDIS_SOCK" 2>/dev/null || echo "???")
    SOCK_OWNER=$(stat -c '%U:%G' "$REDIS_SOCK" 2>/dev/null || echo "???")
    ok "Права: ${SOCK_PERMS} владелец: ${SOCK_OWNER}"
else
    fail "Сокет НЕ найден: ${REDIS_SOCK}"
    if redis-cli ping 2>/dev/null | grep -q PONG; then
        warning "Redis отвечает по TCP — сокет ещё не настроен"
        info "Запусти: sudo bash $0  (без --check)"
    else
        fail "Redis не отвечает ни по socket ни по TCP"
    fi
fi

PONG=$($CLI ping 2>/dev/null || echo "FAIL")
if [[ "$PONG" == "PONG" ]]; then
    ok "PING → PONG"
else
    fail "PING не отвечает (ответ: ${PONG})"
    echo -e "\n  ${RED}Redis недоступен. Остальные проверки невозможны.${NC}"
    echo -e "  Пройдено: ${pass} | Внимание: ${warn_count} | Ошибки: ${fail}"
    exit 1
fi

# Версия (если --check без setup)
if [[ "$REDIS_MAJOR" -eq 0 ]]; then
    REDIS_VERSION=$($CLI info server 2>/dev/null | grep "^redis_version:" | cut -d: -f2 | tr -d '\r' || echo "0.0.0")
    REDIS_MAJOR=$(echo "$REDIS_VERSION" | cut -d. -f1)
    info "Redis v${REDIS_VERSION}"
fi

# --- 6.2 Латентность ---
echo -e "\n${BOLD}  Латентность${NC}"

LATENCIES=()
for _i in $(seq 1 10); do
    START_NS=$(date +%s%N)
    $CLI ping >/dev/null 2>&1
    END_NS=$(date +%s%N)
    DIFF_US=$(( (END_NS - START_NS) / 1000 ))
    LATENCIES+=("$DIFF_US")
done

IFS=$'\n' SORTED=($(printf '%s\n' "${LATENCIES[@]}" | sort -n)); unset IFS
MEDIAN=${SORTED[4]}
MEDIAN_MS=$(echo "scale=3; $MEDIAN / 1000" | bc 2>/dev/null || echo "?")
MIN_L=${SORTED[0]}
MAX_L=${SORTED[9]}
MIN_MS=$(echo "scale=3; $MIN_L/1000" | bc 2>/dev/null || echo "?")
MAX_MS=$(echo "scale=3; $MAX_L/1000" | bc 2>/dev/null || echo "?")

if [[ "$MEDIAN" -lt 1000 ]]; then
    ok "PING медиана: ${MEDIAN_MS}ms (< 1ms)"
elif [[ "$MEDIAN" -lt 5000 ]]; then
    warning "PING медиана: ${MEDIAN_MS}ms (1-5ms)"
else
    fail "PING медиана: ${MEDIAN_MS}ms (> 5ms)"
fi
info "Мин/макс: ${MIN_MS}ms / ${MAX_MS}ms"

# --- 6.3 Конфигурация ---
echo -e "\n${BOLD}  Конфигурация${NC}"

check_config() {
    local param="$1" expected="$2" label="$3"
    local actual
    actual=$($CLI config get "$param" 2>/dev/null | tail -1 || echo "ERROR")
    if [[ "$actual" == "$expected" ]]; then
        ok "${label}: ${actual}"
    else
        fail "${label}: ожидал '${expected}', получил '${actual}'"
    fi
}

PORT_VAL=$($CLI config get port 2>/dev/null | tail -1 || echo "?")
if [[ "$PORT_VAL" == "0" ]]; then
    ok "TCP отключён (port 0)"
else
    warning "TCP включён: port=${PORT_VAL}"
fi

SOCK_VAL=$($CLI config get unixsocket 2>/dev/null | tail -1 || echo "")
if [[ -n "$SOCK_VAL" ]]; then
    ok "Unix socket: ${SOCK_VAL}"
else
    fail "Unix socket не настроен"
fi

check_config "maxmemory-policy" "volatile-ttl" "Eviction policy"
check_config "hz" "100" "hz"
check_config "lazyfree-lazy-eviction" "yes" "Lazy eviction"
check_config "lazyfree-lazy-expire" "yes" "Lazy expire"
check_config "latency-monitor-threshold" "5" "Latency monitor"

SAVE_VAL=$($CLI config get save 2>/dev/null | tail -1 || echo "?")
if [[ -z "$SAVE_VAL" ]]; then
    ok "RDB persistence отключена"
else
    fail "RDB включена: save='${SAVE_VAL}'"
fi

AOF_VAL=$($CLI config get appendonly 2>/dev/null | tail -1 || echo "?")
if [[ "$AOF_VAL" == "no" ]]; then
    ok "AOF отключён"
else
    fail "AOF включён"
fi

if [[ "$REDIS_MAJOR" -ge 7 ]]; then
    IO_VAL=$($CLI config get io-threads 2>/dev/null | tail -1 || echo "0")
    if [[ "${IO_VAL:-0}" -ge 2 ]]; then
        ok "io-threads: ${IO_VAL}"
    else
        warning "io-threads: ${IO_VAL} (рекомендуется 2+)"
    fi
else
    info "io-threads пропущено (Redis < 7)"
fi

KEYS_TEST=$($CLI keys '*' 2>&1 || true)
if echo "$KEYS_TEST" | grep -qi "unknown command\|ERR"; then
    ok "KEYS заблокирована"
else
    warning "KEYS не заблокирована"
fi

# --- 6.4 Память ---
echo -e "\n${BOLD}  Память${NC}"

MEM_INFO=$($CLI info memory 2>/dev/null || echo "")
USED_MEM=$(echo "$MEM_INFO" | grep "^used_memory:" | cut -d: -f2 | tr -d '\r' || echo "0")
USED_MEM_HR=$(echo "$MEM_INFO" | grep "^used_memory_human:" | cut -d: -f2 | tr -d '\r' || echo "?")
MAX_MEM=$(echo "$MEM_INFO" | grep "^maxmemory:" | cut -d: -f2 | tr -d '\r' || echo "0")
MAX_MEM_HR=$(echo "$MEM_INFO" | grep "^maxmemory_human:" | cut -d: -f2 | tr -d '\r' || echo "?")
FRAG=$(echo "$MEM_INFO" | grep "^mem_fragmentation_ratio:" | cut -d: -f2 | tr -d '\r' || echo "1.00")

if [[ "${MAX_MEM:-0}" -gt 0 ]]; then
    MEM_PCT=$((USED_MEM * 100 / MAX_MEM))
    if [[ "$MEM_PCT" -lt 60 ]]; then
        ok "Память: ${USED_MEM_HR} / ${MAX_MEM_HR} (${MEM_PCT}%)"
    elif [[ "$MEM_PCT" -lt 80 ]]; then
        warning "Память: ${USED_MEM_HR} / ${MAX_MEM_HR} (${MEM_PCT}%)"
    else
        fail "Память: ${USED_MEM_HR} / ${MAX_MEM_HR} (${MEM_PCT}% — критично)"
    fi
else
    warning "maxmemory не установлен"
fi

FRAG_OK=$(echo "$FRAG < 1.5" | bc 2>/dev/null || echo "1")
if [[ "$FRAG_OK" == "1" ]]; then
    ok "Фрагментация: ${FRAG}"
else
    FRAG_CRIT=$(echo "$FRAG > 2.0" | bc 2>/dev/null || echo "0")
    if [[ "$FRAG_CRIT" == "1" ]]; then
        fail "Фрагментация: ${FRAG} (> 2.0)"
    else
        warning "Фрагментация: ${FRAG} (> 1.5)"
    fi
fi

STATS_INFO=$($CLI info stats 2>/dev/null || echo "")
EVICTED=$(echo "$STATS_INFO" | grep "^evicted_keys:" | cut -d: -f2 | tr -d '\r' || echo "0")
if [[ "${EVICTED:-0}" == "0" ]]; then
    ok "Evicted keys: 0"
else
    fail "Evicted keys: ${EVICTED} — данные теряются!"
fi

# --- 6.5 Клиенты ---
echo -e "\n${BOLD}  Клиенты${NC}"

CLIENT_INFO=$($CLI info clients 2>/dev/null || echo "")
CONNECTED=$(echo "$CLIENT_INFO" | grep "^connected_clients:" | cut -d: -f2 | tr -d '\r' || echo "0")
BLOCKED=$(echo "$CLIENT_INFO" | grep "^blocked_clients:" | cut -d: -f2 | tr -d '\r' || echo "0")

if [[ "${CONNECTED:-0}" -le 30 ]]; then
    ok "Клиентов: ${CONNECTED}"
else
    warning "Клиентов: ${CONNECTED} (многовато)"
fi

if [[ "${BLOCKED:-0}" == "0" ]]; then
    ok "Заблокированных: 0"
else
    fail "Заблокированных: ${BLOCKED}"
fi

# --- 6.6 Slowlog ---
echo -e "\n${BOLD}  Slowlog${NC}"

SLOWLOG_LEN=$($CLI slowlog len 2>/dev/null | tr -d '\r' || echo "0")
if [[ "${SLOWLOG_LEN:-0}" == "0" ]]; then
    ok "Slowlog пуст"
else
    warning "Slowlog: ${SLOWLOG_LEN} записей"
    info "Последние 3:"
    $CLI slowlog get 3 2>/dev/null | head -20 | while IFS= read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done
fi

# --- 6.7 ОС ---
echo -e "\n${BOLD}  ОС${NC}"

THP_STATUS=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "unknown")
if echo "$THP_STATUS" | grep -q '\[never\]'; then
    ok "THP отключены"
else
    fail "THP включены: ${THP_STATUS}"
fi

OC_VAL=$(sysctl -n vm.overcommit_memory 2>/dev/null || echo "?")
if [[ "$OC_VAL" == "1" ]]; then
    ok "vm.overcommit_memory = 1"
else
    fail "vm.overcommit_memory = ${OC_VAL} (должен быть 1)"
fi

SOMAX_VAL=$(sysctl -n net.core.somaxconn 2>/dev/null || echo "0")
if [[ "${SOMAX_VAL:-0}" -ge 512 ]]; then
    ok "net.core.somaxconn = ${SOMAX_VAL}"
else
    warning "net.core.somaxconn = ${SOMAX_VAL} (рекомендуется 1024+)"
fi

if [[ "$APP_USER" != "root" ]] && id "$APP_USER" &>/dev/null; then
    if groups "$APP_USER" 2>/dev/null | grep -qw "$REDIS_GROUP"; then
        ok "${APP_USER} в группе ${REDIS_GROUP}"
    else
        fail "${APP_USER} НЕ в группе ${REDIS_GROUP}"
    fi
fi

# --- 6.8 Бенчмарк ---
echo -e "\n${BOLD}  Бенчмарк (1000 HSET pipeline)${NC}"

BENCH_START=$(date +%s%N)
{
    for i in $(seq 1 1000); do
        printf 'HSET _bench_%s b 45000.10 a 45000.20 t 1741234567890\r\n' "$i"
    done
} | $CLI --pipe >/dev/null 2>&1 || warning "Pipeline write частично не удался"
BENCH_END=$(date +%s%N)
BENCH_WRITE=$(( (BENCH_END - BENCH_START) / 1000000 ))

READ_START=$(date +%s%N)
{
    for i in $(seq 1 1000); do
        printf 'HMGET _bench_%s b a t\r\n' "$i"
    done
} | $CLI --pipe >/dev/null 2>&1 || true
READ_END=$(date +%s%N)
BENCH_READ=$(( (READ_END - READ_START) / 1000000 ))

# Cleanup
{
    for i in $(seq 1 1000); do
        printf 'DEL _bench_%s\r\n' "$i"
    done
} | $CLI --pipe >/dev/null 2>&1 || true

if [[ "$BENCH_WRITE" -lt 100 ]]; then
    ok "Write 1000 HSET: ${BENCH_WRITE}ms"
elif [[ "$BENCH_WRITE" -lt 500 ]]; then
    ok "Write 1000 HSET: ${BENCH_WRITE}ms (норм)"
else
    warning "Write 1000 HSET: ${BENCH_WRITE}ms (медленно)"
fi

if [[ "$BENCH_READ" -lt 100 ]]; then
    ok "Read 1000 HMGET: ${BENCH_READ}ms"
elif [[ "$BENCH_READ" -lt 500 ]]; then
    ok "Read 1000 HMGET: ${BENCH_READ}ms (норм)"
else
    warning "Read 1000 HMGET: ${BENCH_READ}ms (медленно)"
fi

if [[ "$BENCH_WRITE" -gt 0 ]]; then
    WRITE_OPS=$((1000 * 1000 / BENCH_WRITE))
    info "~${WRITE_OPS}K write ops/s"
fi

# ============================================================================
#  ИТОГИ
# ============================================================================

echo ""
echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${GREEN}✓ Пройдено:${NC}  ${pass}"
echo -e "  ${YELLOW}⚠ Внимание:${NC}  ${warn_count}"
echo -e "  ${RED}✗ Ошибки:${NC}    ${fail}"
echo ""

if [[ "$fail" -eq 0 ]] && [[ "$warn_count" -eq 0 ]]; then
    echo -e "  ${GREEN}${BOLD}Всё идеально ✓${NC}"
elif [[ "$fail" -eq 0 ]]; then
    echo -e "  ${GREEN}${BOLD}Redis работает ✓${NC}  (есть предупреждения)"
else
    echo -e "  ${RED}${BOLD}Есть ошибки — исправь их${NC}"
fi

echo ""
echo -e "  Подключение из Python:"
echo -e "  ${CYAN}r = redis.Redis(unix_socket_path='${REDIS_SOCK}', decode_responses=False)${NC}"
echo ""

REPORT_FILE="/tmp/redis_health_$(date +%Y%m%d_%H%M%S).txt"
{
    echo "Redis Health Report — $(date)"
    echo "Redis: v${REDIS_VERSION}"
    echo "Pass: ${pass} | Warn: ${warn_count} | Fail: ${fail}"
    echo "Socket: ${REDIS_SOCK}"
    echo "Ping median: ${MEDIAN_MS:-?}ms (min ${MIN_MS:-?}ms, max ${MAX_MS:-?}ms)"
    echo "Memory: ${USED_MEM_HR:-?} / ${MAX_MEM_HR:-?}"
    echo "Fragmentation: ${FRAG:-?}"
    echo "Evicted: ${EVICTED:-0}"
    echo "Clients: ${CONNECTED:-?}"
    echo "Benchmark write: ${BENCH_WRITE}ms / 1000 HSET"
    echo "Benchmark read: ${BENCH_READ}ms / 1000 HMGET"
} > "$REPORT_FILE"

info "Отчёт: ${REPORT_FILE}"
echo ""

exit "$fail"
