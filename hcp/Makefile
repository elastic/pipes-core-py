SHELL := bash
TEE_STDERR := tee >(cat 1>&2)

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
