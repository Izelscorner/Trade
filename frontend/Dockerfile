FROM node:22-alpine AS builder

WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci --no-audit
COPY . .
RUN npm run build

FROM nginx:alpine

# Create writable dirs for non-root nginx
RUN mkdir -p /var/cache/nginx /var/log/nginx /tmp/nginx \
    && chown -R nginx:nginx /var/cache/nginx /var/log/nginx /tmp/nginx /var/run /run \
    && chmod -R 755 /var/cache/nginx /var/log/nginx /tmp/nginx /var/run /run

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Set PID to a writable location
RUN sed -i 's|/run/nginx.pid|/tmp/nginx/nginx.pid|g' /etc/nginx/nginx.conf

# Copy our nginx config and built files
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /build/dist /usr/share/nginx/html

USER nginx

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
