FROM node:22-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --omit=dev

COPY client ./client
COPY server ./server

ENV NODE_ENV=production
ENV PORT=3000
ENV KAFKA_BROKERS=kafka:29092

EXPOSE 3000

CMD ["node", "server/server.js"]