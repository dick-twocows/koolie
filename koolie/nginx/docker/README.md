#Docker

##To run the NGINX in Docker

Bind 'nginx.conf' so we do not lose the other files (ie mimes.conf) in '/etc/nginx'.
Bind 'servers' folder.

### Just the 'nginx.conf'

docker run --name nginx --restart always -d -p 8080:80 --mount src=/home/dick/PycharmProjects/koolie/koolie/nginx/docker/etc/nginx/nginx.conf,target=/etc/nginx/nginx.conf,type=bind nginx:latest

### With a mounted 'servers' folder

docker run --name nginx --restart always -d -p 8080:80 --mount src=/home/dick/PycharmProjects/koolie/koolie/nginx/docker/etc/nginx/nginx.conf,target=/etc/nginx/nginx.conf,type=bind --mount src=/home/dick/PycharmProjects/koolie/koolie/nginx/docker/etc/nginx/servers,target=/etc/nginx/servers,type=bind nginx:latest

##Check it is running

wget -O - http://localhost:8080/status/

##Check the NGINX configuration

docker exec -i nginx nginx -T

docker exec -it nginx ls -l /etc/nginx/

docker exec -it nginx ls -l /etc/nginx/servers/