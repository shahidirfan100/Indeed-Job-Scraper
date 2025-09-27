FROM alpine:latest

RUN apk add --no-cache nodejs npm

RUN addgroup app && adduser app -G app -D
WORKDIR /home/app
USER app

COPY --chown=app:app package*.json ./
RUN npm i --omit=dev && rm -r ~/.npm || true

COPY --chown=app:app . ./

ENV APIFY_LOG_LEVEL=ERROR

CMD npm start --silent
