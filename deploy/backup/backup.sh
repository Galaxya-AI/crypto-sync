#!/usr/bin/env bash
# Daily MariaDB logical backup with optional S3 upload.
#
# Required env (set in docker-compose):
#   DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
#
# Optional:
#   BACKUP_S3_BUCKET   — if set, gzip+upload then delete local file
#   BACKUP_RETENTION_DAYS — default 7, applies to the local /backups dir
#
# Logical backup (mysqldump) is used because mariabackup needs a privileged
# socket on the source container; mysqldump works over the wire and is
# perfectly fine at our data scale (millions of rows, not billions).

set -euo pipefail

LOG_PREFIX="$(date -Iseconds) [backup]"
echo "${LOG_PREFIX} starting"

BACKUP_DIR="/backups"
mkdir -p "${BACKUP_DIR}"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_FILE="${BACKUP_DIR}/${DB_NAME}-${TIMESTAMP}.sql.gz"

mysqldump \
  --host="${DB_HOST}" \
  --port="${DB_PORT}" \
  --user="${DB_USER}" \
  --password="${DB_PASSWORD}" \
  --single-transaction \
  --routines \
  --triggers \
  --events \
  --default-character-set=utf8mb4 \
  "${DB_NAME}" \
  | gzip -9 > "${LOCAL_FILE}"

LOCAL_SIZE="$(du -h "${LOCAL_FILE}" | cut -f1)"
echo "${LOG_PREFIX} dump_done file=${LOCAL_FILE} size=${LOCAL_SIZE}"

if [[ -n "${BACKUP_S3_BUCKET:-}" ]]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "${LOG_PREFIX} skipping_s3_upload reason=awscli_not_installed bucket=${BACKUP_S3_BUCKET}" >&2
  else
    S3_KEY="mariadb/${DB_NAME}/$(date -u +%Y/%m/%d)/${DB_NAME}-${TIMESTAMP}.sql.gz"
    echo "${LOG_PREFIX} uploading_to_s3 bucket=${BACKUP_S3_BUCKET} key=${S3_KEY}"
    aws s3 cp "${LOCAL_FILE}" "s3://${BACKUP_S3_BUCKET}/${S3_KEY}"
    rm -f "${LOCAL_FILE}"
  fi
fi

# Local retention cleanup
RETENTION="${BACKUP_RETENTION_DAYS:-7}"
find "${BACKUP_DIR}" -name "${DB_NAME}-*.sql.gz" -mtime +"${RETENTION}" -delete
echo "${LOG_PREFIX} done retention_days=${RETENTION}"
