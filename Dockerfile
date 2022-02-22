FROM ghcr.io/praekeltfoundation/python-base-nw:3.10-bullseye

RUN apt-get-install.sh cron

COPY crontab.txt /etc/cron.d/crontab.txt

RUN chmod 0644 /etc/cron.d/crontab.txt

RUN crontab /etc/cron.d/crontab.txt

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD echo -n 'Start time: ' >> /var/log/cron.log \
    && date >> /var/log/cron.log \
    && cron \
    && tail -f /var/log/cron.log
