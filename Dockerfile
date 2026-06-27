# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Multi-stage build for the PgPulse Fastify app.
# ---------------------------------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY tsconfig.json ./
COPY src ./src
COPY migrations ./migrations
RUN npm run build

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package.json ./
RUN npm install --omit=dev
COPY --from=build /app/dist ./dist
COPY --from=build /app/migrations ./migrations
# tsx is needed for the migration runner (runs .ts directly).
RUN npm install tsx@4.16.2
COPY src ./src
COPY tsconfig.json ./
EXPOSE 3000
CMD ["node", "dist/server.js"]
