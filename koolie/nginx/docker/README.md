#Docker

##To run the NGINX in Docker

Mount 'nginx.conf'.
Bind 'servers' folder.

docker run --name nginx -d -p 8080:80 -v /home/dick/PycharmProjects/koolie/koolie/nginx/docker/nginx.conf:/etc/nginx/nginx.conf --mount src=/home/dick/PycharmProjects/koolie/koolie/nginx/docker/servers,target=/etc/nginx/servers,type=bind nginx:latest

##Check it is running

wget -O - http://localhost:8080/status/

##Check the NGINX configuration

docker exec -i nginx nginx -T