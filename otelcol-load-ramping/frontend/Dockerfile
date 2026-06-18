FROM nginx:1.27-alpine

# Reverse-proxy config (serves static assets, proxies /api to the backend)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Static UI
COPY index.html app.js style.css /usr/share/nginx/html/

EXPOSE 80
