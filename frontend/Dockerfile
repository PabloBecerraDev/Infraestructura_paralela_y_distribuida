# Etapa de build
FROM node:18 as build
WORKDIR /app
COPY . .
RUN npm install
RUN npm run build

# Etapa de producción con nginx
FROM nginx:stable-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
