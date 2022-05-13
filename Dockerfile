FROM rubber4duck/fixbase:latest

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src/app

COPY . .

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y cron \
  && which cron \
  && rm -rf /etc/cron.*/* \
  && crontab crontab.file \
  && chmod 755 update.sh \
  && source $VIRTUAL_ENV/bin/activate

COPY entrypoint.sh /entrypoint.sh

VOLUME [ "/usr/src/app/Fallzahlen" ]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["cron","-f", "-l", "2"]