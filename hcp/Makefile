SHELL := bash
TEE_STDERR := tee >(cat 1>&2)
LOG_DIR ?= .

DOCKER_COMPOSE ?= docker compose
UP_FLAGS ?= --wait --quiet-pull

VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=test
export VAULT_ADDR VAULT_TOKEN

vault-up:
	$(DOCKER_COMPOSE) up $(UP_FLAGS)

vault-down:
	$(DOCKER_COMPOSE) down $(DOWN_FLAGS)

test: vault-up
	echo "secret: mysecret" | elastic-pipes run vault/test.yaml --log-level=debug | [ "`$(TEE_STDERR)`" = "secret: mysecret" ]

logs:
	mkdir -p $(LOG_DIR)
	$(DOCKER_COMPOSE) logs --no-color > $(LOG_DIR)/docker-compose.log 2>&1 || true
	$(DOCKER_COMPOSE) ps --all > $(LOG_DIR)/docker-compose-ps.log 2>&1 || true
	docker inspect $$($(DOCKER_COMPOSE) ps -q) > $(LOG_DIR)/docker-inspect.json 2>&1 || true
