FROM rubber4duck/fixbase:latest

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src/app

COPY . .

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y cron curl \
  && which cron \
  && rm -rf /etc/cron.*/* \
  && crontab crontab.file \
  && chmod 755 update.sh

COPY entrypoint.sh /entrypoint.sh

VOLUME [ "/usr/src/app/Fallzahlen" ]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["cron","-f", "-L", "15"]