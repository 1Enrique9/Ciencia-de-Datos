IMAGE_NAME=gitwars-superheroes
CONTAINER_NAME=gitwars-superheroes

build:
	docker build -t $(IMAGE_NAME) -f deployments/Dockerfile .

run:
	-docker stop $(CONTAINER_NAME)
	-docker rm $(CONTAINER_NAME)
	docker run --name $(CONTAINER_NAME) -p 8000:8000 $(IMAGE_NAME)

status:
	docker ps -a | grep $(CONTAINER_NAME) || true

stop:
	-docker stop $(CONTAINER_NAME)
	-docker rm $(CONTAINER_NAME)

clean:
	-docker rmi $(IMAGE_NAME)

package:
	cd .. && tar --exclude=".git" --exclude=".github" \
	    -czvf equipo_wework.tar.gz gitwars-superheroes-lab10
