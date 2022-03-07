# Run tests
.PHONY: test
test:
	# -docker kill $$(docker ps -q)
	-docker container rm $$(docker ps -aq) -f
	-docker network rm $$(docker network ls --filter "name=mongo-set_default" -q)
	# go test runs tests in parallel for different packages. to avoid that we set -p
	go test -race -count=1 -v -p 1 ./...

.PHONY: generate
generate:
	./codegen.sh ./api/proto/*.proto
