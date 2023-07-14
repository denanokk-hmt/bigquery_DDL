 
FROM golang:1.17-alpine

ENV NODE_ENV=prd
ARG COMMITID
ENV COMMITID ${COMMITID}
ARG SHA_COMMIT_ID
ENV SHA_COMMIT_ID ${SHA_COMMIT_ID}
ARG VERSION
ENV VERSION ###VERSION###
ARG DEPLOY_UNIXTIME
ENV DEPLOY_UNIXTIME ###DEPLOY_UNIXTIME###

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

RUN apk add --update --no-cache ca-certificates git
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o bin/fbigquery-ddl cmd/main.go

#EXPOSE 9090

CMD "bin/bigquery-ddl"