#!/bin/bash

# Setup do Pipeline CDC - CNES
# Uso: ./scripts/setup.sh

set -e

CONNECT_URL="http://localhost:8083"
CONFIG_DIR="./config"

echo "==="
echo "TCC Lakehouse CNES - Setup do Pipeline"
echo "$(date)"
echo "==="
echo ""

# 1. Verifica se os servicos do docker estao no ar
echo "[1/4] Verificando servicos..."

wait_for_service() {
    local name=$1
    local url=$2
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "  ✅ $name esta pronto"
            return 0
        fi
        attempt=$((attempt + 1))
        echo "  ⏳ Aguardando $name... ($attempt/$max_attempts)"
        sleep 5
    done

    echo "  ❌ $name nao respondeu apos $max_attempts tentativas"
    return 1
}

wait_for_service "Kafka Connect" "$CONNECT_URL/connectors"
wait_for_service "Kafka UI" "http://localhost:8080"

echo ""

# 2. Remove conector antigo (se existir)
echo "[2/4] Verificando conectores existentes..."

EXISTING=$(curl -sf "$CONNECT_URL/connectors" 2>/dev/null || echo "[]")
echo "Conectores atuais: $EXISTING"

if echo "$EXISTING" | grep -q "cnes-mysql-source"; then
    echo "Removendo conector antigo..."
    curl -sf -X DELETE "$CONNECT_URL/connectors/cnes-mysql-source" > /dev/null
    sleep 3
    echo "✅ Conector removido"
fi

echo ""

# 3. Registra o conector Debezium
echo "[3/4] Registrando conector Debezium..."

RESPONSE=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    "$CONNECT_URL/connectors" \
    -d @"$CONFIG_DIR/debezium-mysql-source.json" 2>&1)

if echo "$RESPONSE" | grep -q '"name"'; then
    echo "  ✅ Conector registrado com sucesso!"
else
    echo "  ❌ Erro ao registrar conector:"
    echo "  $RESPONSE"
    exit 1
fi

sleep 5

# 4. Verifica status do conector
echo ""
echo "[4/4] Verificando status do conector..."

STATUS=$(curl -sf "$CONNECT_URL/connectors/cnes-mysql-source/status")
CONNECTOR_STATE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
TASK_STATE=$(echo "$STATUS" | python3 -c "import sys,json; tasks=json.load(sys.stdin)['tasks']; print(tasks[0]['state'] if tasks else 'NO_TASKS')" 2>/dev/null || echo "UNKNOWN")

echo "  Connector: $CONNECTOR_STATE"
echo "  Task: $TASK_STATE"

if [ "$CONNECTOR_STATE" = "RUNNING" ] && [ "$TASK_STATE" = "RUNNING" ]; then
    echo ""
    echo "==="
    echo "  ✅ Pipeline CNES configurado com sucesso!"
    echo ""
    echo "  Topicos Kafka criados:"
    echo "    - cdc.cnes_db.estabelecimentos"
    echo "    - cdc.cnes_db.profissionais"
    echo "    - cdc.cnes_db.carga_horaria"
    echo "    - cdc.cnes_db.equipes"
    echo "    - cdc.cnes_db.equipe_profissionais"
    echo "    - cdc.cnes_db.estab_equipamentos"
    echo "    - cdc.cnes_db.estab_servicos"
    echo ""
    echo "  Proximos passos:"
    echo "    1. Instale deps: docker exec -it php-app composer install"
    echo "    2. Inicie o consumer: docker exec -it php-app php consumer.php --verbose"
    echo "    3. Gere carga: docker exec -it php-app php load_generator.php --scenario=low"
    echo "    4. Veja metricas: docker exec -it php-app php metrics_report.php"
    echo ""
    echo "  Interfaces web:"
    echo "    - Kafka UI: http://localhost:8080"
    echo "    - MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
    echo "==="
else
    echo ""
    echo "  ⚠️  Conector nao esta totalmente operacional."
    echo "  Verifique os logs: docker logs kafka-connect"
fi
