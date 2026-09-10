#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Recriando containers do zero (schema + dados fake serão reprocessados)..."
docker compose down -v
docker compose up -d --force-recreate

echo "Aguardando MySQL ficar pronto..."
for i in $(seq 1 60); do
  if docker exec dw_mysql mysqladmin ping -uroot -pmysql --silent >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Pronto. Postgres em localhost:5432 (northwind) e MySQL em localhost:3306 (mercearia)."
