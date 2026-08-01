.PHONY: build node1 node2 node3 test-single test-ring kill-cluster clean

BINARY_NAME=kvstore
CMD_DIR=./cmd/kvstore

build:
	@echo "Building the Kvstore binary..."
	@go build -o $(BINARY_NAME) $(CMD_DIR)
	@echo "Build complete! Artifact created: ./$(BINARY_NAME)"

# Testing a standalone node (No peers) with Task A1 config flags
test-single: build
	@echo "Booting a Standalone Node for Command Testing..."
	./$(BINARY_NAME) start --node-id 1 --port 6379 --raft-port 10001 --peer-ips "" --max-clients 5000

# 3-Node Cluster Setup using Task A1 Dynamic Configuration flags
node1: build
	@echo "Booting Node 1 (Epoll: 6379, Raft: 10001)..."
	./$(BINARY_NAME) start --node-id 1 --port 6379 --raft-port 10001 --peer-ips "127.0.0.1:10002,127.0.0.1:10003" --max-clients 5000

node2: build
	@echo "Booting Node 2 (Epoll: 6380, Raft: 10002)..."
	./$(BINARY_NAME) start --node-id 2 --port 6380 --raft-port 10002 --peer-ips "127.0.0.1:10001,127.0.0.1:10003" --max-clients 5000

node3: build
	@echo "Booting Node 3 (Epoll: 6381, Raft: 10003)..."
	./$(BINARY_NAME) start --node-id 3 --port 6381 --raft-port 10003 --peer-ips "127.0.0.1:10001,127.0.0.1:10002" --max-clients 5000

test-ring:
	@echo "Running Consistent Hashing Simulation..."
	@cd chashing && go test -v

kill-cluster:
	@echo "Hunting down and terminating ghost nodes..."
	-fuser -k 6379/tcp 6380/tcp 6381/tcp 2>/dev/null || true
	-fuser -k 10001/tcp 10002/tcp 10003/tcp 2>/dev/null || true
	@echo "Ports cleared."

clean: kill-cluster
	@echo "Cleaning up binaries and AOF files..."
	@rm -f $(BINARY_NAME)
	@rm -f *.aof
	@echo "Environment pristine."