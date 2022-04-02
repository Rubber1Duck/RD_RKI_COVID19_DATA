FROM python:3.9.12-alpine

WORKDIR /usr/src/app

COPY . .

RUN apk update \
  && apk upgrade \
  && apk add \
    --virtual .dependencies build-base binutils \
  && pip install --no-cache-dir -r requirements.txt \
  && apk del .dependencies

CMD []
#CMD [ "python", "./your-daemon-or-script.py" ]