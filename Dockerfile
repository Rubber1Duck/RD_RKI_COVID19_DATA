FROM python:3.9-slim

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src/app

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY . .

RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get -y install --no-install-recommends apt-utils \
  && apt-get -y install --no-install-recommends cron curl wget build-essential manpages-dev xz-utils nano \
  && source $VIRTUAL_ENV/bin/activate \
  && pip install --no-cache-dir -r requirements.txt \
  && which cron \
  && rm -rf /etc/cron.*/* \
  && crontab crontab1.file \
  && chmod 755 update1.sh update2.sh entrypoint.sh \
  && apt-get -y purge wget build-essential manpages-dev \
  && apt-get -y --purge autoremove \
  && apt-get clean \
  && apt-get autoclean

VOLUME [ "/usr/src/app/dataStore", "/usr/src/app/data" ]

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["cron","-f", "-L", "15"]
