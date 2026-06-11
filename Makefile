.PHONY: build node1 node2 node3 test-ring kill-cluster clean

BINARY_NAME=kvstore
CLUSTER_NODES=127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002

build:
	@echo "Building the Kvstore binary..."
	@go build -o $(BINARY_NAME) main.go

node1: build
	@echo "Booting Node 1 (Port 9000)..."
	./$(BINARY_NAME) -port=9000 -cluster="$(CLUSTER_NODES)"

node2: build
	@echo "Booting Node 2 (Port 9001)..."
	./$(BINARY_NAME) -port=9001 -cluster="$(CLUSTER_NODES)"

node3: build
	@echo "Booting Node 3 (Port 9002)..."
	./$(BINARY_NAME) -port=9002 -cluster="$(CLUSTER_NODES)"

test-ring:
	@echo "Running Consistent Hashing Simulation..."
	@cd chashing && go test -v

kill-cluster:
	@echo "Hunting down and terminating ghost nodes on ports 9000, 9001, 9002..."
	-fuser -k 9000/tcp 2>/dev/null || true
	-fuser -k 9001/tcp 2>/dev/null || true
	-fuser -k 9002/tcp 2>/dev/null || true
	@echo "Ports cleared."

clean: kill-cluster
	@echo "Cleaning up binaries and AOF files..."
	@rm -f $(BINARY_NAME)
	@rm -f *.aof