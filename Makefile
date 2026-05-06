.PHONY: setup discover keks schemas topics web kill \
        produce-csfle produce-cspe \
        consume-csfle-auth consume-csfle-unauth \
        consume-cspe-auth  consume-cspe-unauth \
        clean-keys clean-rbac

# Load .env (CC IDs/keys, KEK ARNs, topic names) and AWS session creds.
-include .env
-include config/aws-session.env
export

CP_HOME       := $(HOME)/confluent-8.2.0
PRODUCER      := $(CP_HOME)/bin/kafka-json-schema-console-producer
CONSUMER      := $(CP_HOME)/bin/kafka-json-schema-console-consumer
JAVA_HOME     := /opt/homebrew/opt/openjdk@21
EXECUTOR      := io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor

# ── Setup ────────────────────────────────────────────────────────────────────
setup: discover keks schemas topics
	@echo ""
	@echo "Setup complete. 'make web' or 'bash startup.sh' to launch the UI."

discover:
	bash scripts/00_discover_env.sh

keks:
	bash scripts/01_setup_keks.sh

schemas:
	bash scripts/02_register_schemas.sh

topics:
	bash scripts/03_create_topics.sh

# ── Web UI ───────────────────────────────────────────────────────────────────
web:
	@pkill -f "demo-csfle-cspe-cloud/web/server.py" 2>/dev/null || true
	@sleep 1
	@(sleep 2 && open http://localhost:8893) &
	python3 web/server.py

kill:
	@pkill -f "demo-csfle-cspe-cloud/web/server.py" 2>/dev/null && echo "stopped" || echo "not running"

# ── Produce ──────────────────────────────────────────────────────────────────
# Both producers stream sample mortgage records as JSON. Encryption is driven
# by the schema's ruleSet (registered earlier by scripts/02).
produce-csfle:
	@test -n "$(CSFLE_TOPIC)" || (echo "ERROR: run 'make discover' first"; exit 1)
	@echo "→ producing to $(CSFLE_TOPIC) (CSFLE: ssn encrypted at field level) ..."
	JAVA_HOME=$(JAVA_HOME) $(PRODUCER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(CSFLE_TOPIC) \
	  --producer.config <(envsubst < config/producer-csfle.properties) \
	  --reader-property schema.registry.url=$(SR_URL) \
	  --reader-property basic.auth.credentials.source=USER_INFO \
	  --reader-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --reader-property value.schema.id=$$(curl -sf -u $(SR_API_KEY):$(SR_API_SECRET) "$(SR_URL)/subjects/$(CSFLE_TOPIC)-value/versions/latest" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])") \
	  --reader-property auto.register.schemas=false \
	  --reader-property "rule.executors=_default_" \
	  --reader-property "rule.executors._default_.class=$(EXECUTOR)" \
	  < data/mortgage-records.json

produce-cspe:
	@test -n "$(CSPE_TOPIC)" || (echo "ERROR: run 'make discover' first"; exit 1)
	@echo "→ producing to $(CSPE_TOPIC) (CSPE: whole payload encrypted) ..."
	JAVA_HOME=$(JAVA_HOME) $(PRODUCER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(CSPE_TOPIC) \
	  --producer.config <(envsubst < config/producer-cspe.properties) \
	  --reader-property schema.registry.url=$(SR_URL) \
	  --reader-property basic.auth.credentials.source=USER_INFO \
	  --reader-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --reader-property value.schema.id=$$(curl -sf -u $(SR_API_KEY):$(SR_API_SECRET) "$(SR_URL)/subjects/$(CSPE_TOPIC)-value/versions/latest" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])") \
	  --reader-property auto.register.schemas=false \
	  --reader-property "rule.executors=_default_" \
	  --reader-property "rule.executors._default_.class=$(EXECUTOR)" \
	  < data/mortgage-records.json

# ── Consume ──────────────────────────────────────────────────────────────────
# Authorized: AWS creds in env → KMS unwraps DEK → plaintext.
# Unauthorized: AWS creds STRIPPED → KMS denies → onFailure=NONE → ciphertext.

consume-csfle-auth:
	@echo "[CSFLE-WITH-KEK] $(CSFLE_TOPIC) — ssn DECRYPTED  (Ctrl-C to stop)"
	JAVA_HOME=$(JAVA_HOME) $(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(CSFLE_TOPIC) --from-beginning --group demo-csfle-with-kek \
	  --consumer.config <(envsubst < config/consumer-csfle-auth.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --formatter-property "rule.executors=_default_" \
	  --formatter-property "rule.executors._default_.class=$(EXECUTOR)"

consume-csfle-unauth:
	@echo "[CSFLE-NO-KEK]  $(CSFLE_TOPIC) — ssn CIPHERTEXT  (Ctrl-C to stop)"
	JAVA_HOME=$(JAVA_HOME) \
	AWS_ACCESS_KEY_ID="" AWS_SECRET_ACCESS_KEY="" AWS_SESSION_TOKEN="" \
	$(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(CSFLE_TOPIC) --from-beginning --group demo-csfle-no-kek \
	  --consumer.config <(envsubst < config/consumer-csfle-unauth.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --formatter-property "rule.executors=_default_" \
	  --formatter-property "rule.executors._default_.class=$(EXECUTOR)"

consume-cspe-auth:
	@echo "[CSPE-WITH-KEK] $(CSPE_TOPIC) — full payload PLAINTEXT  (Ctrl-C to stop)"
	JAVA_HOME=$(JAVA_HOME) $(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(CSPE_TOPIC) --from-beginning --group demo-cspe-with-kek \
	  --consumer.config <(envsubst < config/consumer-cspe-auth.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --formatter-property "rule.executors=_default_" \
	  --formatter-property "rule.executors._default_.class=$(EXECUTOR)"

consume-cspe-unauth:
	@echo "[CSPE-NO-KEK]  $(CSPE_TOPIC) — payload OPAQUE CIPHERTEXT  (Ctrl-C to stop)"
	JAVA_HOME=$(JAVA_HOME) \
	AWS_ACCESS_KEY_ID="" AWS_SECRET_ACCESS_KEY="" AWS_SESSION_TOKEN="" \
	$(CONSUMER) \
	  --bootstrap-server $(BOOTSTRAP_SERVERS) \
	  --topic $(CSPE_TOPIC) --from-beginning --group demo-cspe-no-kek \
	  --consumer.config <(envsubst < config/consumer-cspe-unauth.properties) \
	  --formatter-property schema.registry.url=$(SR_URL) \
	  --formatter-property basic.auth.credentials.source=USER_INFO \
	  --formatter-property basic.auth.user.info=$(SR_API_KEY):$(SR_API_SECRET) \
	  --formatter-property "rule.executors=_default_" \
	  --formatter-property "rule.executors._default_.class=$(EXECUTOR)"

# Delete the 3 service accounts the wizard's RBAC step minted. Cascades on
# the CC side to delete each SA's API keys + role bindings. After this runs,
# the cmd builders fall back to the OrgAdmin keys (KAFKA_API_KEY/SR_API_KEY)
# until the next RBAC step run mints fresh per-role principals.
clean-rbac:
	@# Delete any SA_ID env vars present, both the new 6 (per-topic per-role)
	@# and the legacy 3 (pre-split refactor). Cascades to API keys + bindings.
	@for sa in $(CSFLE_PRODUCER_SA_ID) $(CSPE_PRODUCER_SA_ID) \
	          $(CSFLE_CONSUMER_KEK_SA_ID) $(CSFLE_CONSUMER_NOKEK_SA_ID) \
	          $(CSPE_CONSUMER_KEK_SA_ID) $(CSPE_CONSUMER_NOKEK_SA_ID) \
	          $(PRODUCER_SA_ID) $(CONSUMER_KEK_SA_ID) $(CONSUMER_NOKEK_SA_ID); do \
	  if [ -n "$$sa" ]; then \
	    echo "→ deleting service account $$sa"; \
	    confluent iam service-account delete "$$sa" --force 2>&1 | tail -1; \
	  fi; \
	done
	@echo ""
	@echo "  ✓ SAs deleted (their API keys + role bindings cascaded automatically)."
	@echo "  Remember to clear the 30 *_SA_ID/*_API_KEY/*_API_SECRET lines from .env"
	@echo "  if you want to re-run the RBAC step from scratch."

# Schedule both KMS keys for deletion (7-day pending window — alias is removed
# immediately so re-running 01_setup_keks.sh creates fresh keys cleanly).
clean-keys:
	@for kek in $(CSFLE_KEK_NAME) $(CSPE_KEK_NAME); do \
	  arn=$$(aws kms describe-key --key-id alias/$$kek --region $(AWS_REGION) --query 'KeyMetadata.Arn' --output text 2>/dev/null || true); \
	  if [ -n "$$arn" ]; then \
	    aws kms delete-alias --alias-name alias/$$kek --region $(AWS_REGION); \
	    aws kms schedule-key-deletion --key-id "$$arn" --pending-window-in-days 7 --region $(AWS_REGION) >/dev/null; \
	    echo "  ✓ $$kek scheduled for deletion in 7 days"; \
	  else \
	    echo "  - $$kek: alias not found"; \
	  fi; \
	done
