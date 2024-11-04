#!/bin/bash
BACKUP_DESTINATION=/data01/baserow_backups

FILENAME="baserow_backup-$(date -I).tar.gz"
cd "$(dirname "$0")"
docker compose down
docker compose run --rm -v $BACKUP_DESTINATION:/baserow/backups baserow backend-cmd-with-db backup  -f /baserow/backups/$FILENAME
docker compose up -d
