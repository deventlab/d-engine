# docker/jepsen/Makefile
.PHONY: test view report clean

# Configurable parameters
TIME_LIMIT ?= 60
TEST_COMMAND ?= dengine_ctl
NODE1 ?= node1
NODE2 ?= node2
NODE3 ?= node3
ENDPOINTS ?= http://node1:9081,http://node2:9082,http://node3:9083
COMPOSE_FILE ?= ../docker-compose.yml

# Restart Docker Compose stack
restart-stack:
	@echo "Cleaning output directories..."
	@rm -rf ../output/logs/* ../output/db/*
	@echo "Restarting Docker Compose stack..."
	@docker compose -f $(COMPOSE_FILE) down
	@docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for cluster to initialize (10 seconds)..."
	@sleep 10


# Main test target
test: restart-stack
	@echo "Starting Jepsen test with time limit: ${TIME_LIMIT}s"
	docker exec -e SSH_AUTH_SOCK=/ssh-agent docker-jepsen-1 bash -c '\
		  eval "$$(ssh-agent -s)" && \
		  ssh-add /root/.ssh/id_rsa && \
		  lein run test \
		    --node '"${NODE1}"' \
		    --node '"${NODE2}"' \
		    --node '"${NODE3}"' \
		    --endpoints '"${ENDPOINTS}"' \
		    --time-limit '"${TIME_LIMIT}"' \
		    --command '"${TEST_COMMAND}"''
	@echo "Jepsen test finished, checking result..."
	docker exec docker-jepsen-1 bash -c '\
		lein trampoline run -m clojure.main -e "\
			(require '\''[knossos.model :as model])\
			(println \
				(if (-> (clojure.edn/read-string \
					{:readers {\
						'\''knossos.model.Register model/->Register\
						'\''knossos.model.CASRegister model/->CASRegister\
						'\''knossos.model.Inconsistent model/->Inconsistent}\
					}\
					(slurp \"/app/store/latest/results.edn\"))\
				:valid?)\
				\"✅ PASS\" \"❌ FAIL\"))"'

# Set up SSH agent inside container
ssh-setup:
	@echo "Configuring SSH keys..."
	@docker exec docker-jepsen-1 bash -c "\
		eval \$$(ssh-agent -s) && \
		ssh-add /root/.ssh/id_rsa"

# View latest test results
view:
	@latest=$$(readlink ./store/latest); \
	if [ -z "$$latest" ]; then \
		echo "No test results found"; \
		exit 1; \
	fi; \
	open "$$latest/index.html" 2>/dev/null || \
	echo "Open manually: file://$$(pwd)/$$latest/index.html"

# Show path to latest report
report:
	@latest=$$(readlink ./store/latest); \
	if [ -z "$$latest" ]; then \
		echo "No test results available"; \
		exit 1; \
	fi; \
	echo "Latest report: $$(pwd)/$$latest"

# Clean test artifacts
clean:
	rm -rf ./store/*
