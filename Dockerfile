FROM nginx:alpine
COPY index.html /usr/share/nginx/html/index.html
# data/ is bind-mounted at runtime via docker-compose
