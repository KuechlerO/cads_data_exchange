#!/bin/bash
BACKUP_DESTINATION=/data01/baserow_backups

FILENAME="baserow_backup-$(date -I).tar.gz"
cd "$1"
docker compose run -v $BACKUP_DESTINATION:/baserow/backups baserow backend-cmd-with-db backup -f /baserow/backups/$FILENAME

