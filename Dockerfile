FROM rubber4duck/fixbase:latest

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src/app

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y cron curl git \
  && which cron \
  && rm -rf /etc/cron.*/* \
  && rm -rf /usr/src/app/* \
  && git clone https://github.com/Rubber1Duck/RD_RKI_COVID19_DATA.git /usr/src/app \
  && rm -rf .git .github .dockerignore .gitignore README.md requirements.txt Dockerfile.base Dockerfile Fallzahlen/*.csv\
  && apt-get purge -y git \
  && apt-get --purge -y autoremove \
  && apt-get clean \
  && crontab crontab.file \
  && chmod 755 update.sh

COPY entrypoint.sh /entrypoint.sh

VOLUME [ "/usr/src/app/Fallzahlen" ]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["cron","-f", "-L", "15"]