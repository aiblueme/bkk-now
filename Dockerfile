FROM nginx:alpine

RUN rm -rf /usr/share/nginx/html/*

COPY index.html /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/conf.d/default.conf
# data/ is bind-mounted at runtime via docker-compose

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
