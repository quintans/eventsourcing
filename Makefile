# go test runs tests in parallel for different packages. to avoid that we set -p

# Run tests
.PHONY: test
test:
	go test -tags unit -race -count=1 -v ./...

docker-clean:
	# -docker kill $$(docker ps -q)
	-docker container rm $$(docker ps -aq) -f
	-docker network rm $$(docker network ls --filter "name=mongo-set_default" -q)

test-mongo: docker-clean
	go test -tags mongo -race -count=1 -v -p 1 ./...

test-pg: docker-clean
	go test -tags pg -race -count=1 -v -p 1 ./...

test-mysql: docker-clean
	go test -tags mysql -race -count=1 -v -p 1 ./...

test-redis: docker-clean
	go test -tags redis -race -count=1 -v -p 1 ./...

test-consul: docker-clean
	go test -tags consul -race -count=1 -v -p 1 ./...

test-all: test test-redis test-consul test-pg test-mysql test-mongo docker-clean

.PHONY: generate
generate:
	./codegen.sh ./api/proto/*.proto
