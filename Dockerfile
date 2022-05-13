FROM rubber4duck/fixbase:latest

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src/app

COPY . .

RUN apt-get update \
  && apt-get install -y cron \
  && service cron start \
  && crontab crontab.file \
  && chmod 755 update.sh

RUN source $VIRTUAL_ENV/bin/activate

RUN touch /var/log/cron.log

VOLUME [ "/usr/src/app/Fallzahlen" ]

CMD cron && tail -f /var/log/cron.log
