FROM mariadb:11.4

# Tools we need on top of mariadb-client / mariabackup:
#  - tini for clean PID 1 signal handling
#  - cron for the actual scheduling
#  - gzip to compress the dump
#  - awscli (via pip) for the optional S3 upload — apt no longer ships it on
#    recent Debian releases, so we go through pip with --break-system-packages
#    (safe inside a container, isolates nothing else).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      tini \
      cron \
      gzip \
      python3-pip \
 && pip3 install --break-system-packages --no-cache-dir awscli \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/backup
COPY backup.sh /opt/backup/backup.sh
COPY crontab /etc/cron.d/mariadb-backup
RUN chmod +x /opt/backup/backup.sh \
 && chmod 0644 /etc/cron.d/mariadb-backup \
 && touch /var/log/cron.log

ENTRYPOINT ["tini", "--"]
CMD ["bash", "-c", "cron && tail -f /var/log/cron.log"]
