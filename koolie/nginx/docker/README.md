#Docker

##To run the NGINX in Docker;

docker run --name nginx -d -p 8080:80 -v /home/dick/PycharmProjects/koolie/koolie/nginx/docker/nginx.conf:/etc/nginx/nginx.conf -v /home/dick/PycharmProjects/koolie/koolie/nginx/docker/localhost.conf:/etc/nginx/servers/localhost.conf nginx:latest

##Check it is running;

wget -O - http://localhost:8080/status/

##Check the NGINX configuration;

docker exec -i nginx nginx -T