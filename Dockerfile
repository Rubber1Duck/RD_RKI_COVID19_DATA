FROM python:3.9-slim

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src/app

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY . .

ARG SEVENZIP_VERSION="2301"

RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get -y install --no-install-recommends apt-utils \
  && apt-get -y install --no-install-recommends cron curl wget build-essential manpages-dev xz-utils nano jq gawk\
  && ./get7Zip.sh ${SEVENZIP_VERSION} \
  && source $VIRTUAL_ENV/bin/activate \
  && pip install --no-cache-dir -r requirements_submodule.txt \
  && which cron \
  && rm -rf /etc/cron.*/* \
  && crontab crontab1.file \
  && apt-get -y purge wget build-essential manpages-dev xz-utils \
  && apt-get -y --purge autoremove \
  && apt-get clean \
  && apt-get autoclean

VOLUME [ "/usr/src/app/dataStore", "/usr/src/app/data" ]

ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["cron","-f", "-L", "15"]
