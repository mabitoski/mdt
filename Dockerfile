FROM node:18-bullseye AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

FROM node:18-bullseye-slim
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV DATA_DIR=/app/data

COPY --from=build /app /app
RUN mkdir -p /app/data

EXPOSE 3000
CMD ["node", "server.js"]
