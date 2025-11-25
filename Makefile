# Paths
PATH_AIRFLOW := apache-airflow# Directory name for airflow
PATH_SPARK := apache-spark# Directory name for spark
PATH_MINIO := minio# Directory name for minio

# Docker Compose files
FILE_DOCKER_COMPOSE := docker-compose.yaml# Name of the docker-compose file
FILE_AIRFLOW_COMPOSE := $(PATH_AIRFLOW)/$(FILE_DOCKER_COMPOSE)# Path to airflow docker-compose file
FILE_SPARK_COMPOSE := $(PATH_SPARK)/$(FILE_DOCKER_COMPOSE)# Path to spark docker-compose file
FILE_MINIO_COMPOSE := $(PATH_MINIO)/$(FILE_DOCKER_COMPOSE)# Path to minio docker-compose file

# Docker commands
CMD_DOCKER_COMPOSE := docker compose -f# Command to run docker compose with a specific file
CMD_DOCKER_NETWORK := docker network# Command to manage docker networks

# Miscelaneous
SPACE_BAR := '\n%*s\n\n' 100 | tr ' ' -# Space bar for formatting output
NETWORK_NAME := airflow_network# Name of the docker network
MAKFILE := $(lastword $(MAKEFILE_LIST))

# VAR=value  ## Overrides a variable, e.g ARGS_COMPOSE=-a. See helpvars target
ARGS_COMPOSE =#= Additional arguments for the Docker compose commands
ARGS_NETWORK =#= Additional arguments for the Docker network commands
SERVICE_NAME ?=#= Variable for the name of the service, also can be set when calling export SERVICE_NAME=scheduler

# Targets
.PHONY: network init minio airflow spark up ps log_minio log_airflow log_spark down clean get_logs help helpvars 

network:  ## Creates the Docker network
	$(CMD_DOCKER_NETWORK) create $(NETWORK_NAME) $(ARGS_NETWORK) || true
	@printf $(SPACE_BAR)

init: network  ## Initializes Airflow (creates DB, user, etc.) and Spark logs volume
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) up airflow-init $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	docker volume create spark_logs
	@printf $(SPACE_BAR)
	docker run --rm -v spark_logs:/opt/spark/logs alpine chown -R 185:185 /opt/spark/logs
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) build --no-cache $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) down $(ARGS_COMPOSE)

minio: network  ## Starts MinIO service
	$(CMD_DOCKER_COMPOSE) $(FILE_MINIO_COMPOSE) up -d $(ARGS_COMPOSE)

airflow: network  ## Starts Airflow services
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) up -d $(ARGS_COMPOSE)

spark: network  ## Starts Spark service
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) up -d $(ARGS_COMPOSE)

up: network  ## Starts all services
	$(CMD_DOCKER_COMPOSE) $(FILE_MINIO_COMPOSE) up -d $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) up -d $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) up -d $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)

ps:  ## Shows the status of all services
	$(CMD_DOCKER_COMPOSE) $(FILE_MINIO_COMPOSE) ps $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) ps $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) ps $(ARGS_COMPOSE)

log_minio:  ## Shows the logs of minio services
	$(CMD_DOCKER_COMPOSE) $(FILE_MINIO_COMPOSE) logs -f $(ARGS_COMPOSE) $(SERVICE_NAME)

log_airflow:  ## Shows the logs of airflow services
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) logs -f $(ARGS_COMPOSE) $(SERVICE_NAME)

log_spark:  ## Shows the logs of spark services
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) logs -f $(ARGS_COMPOSE) $(SERVICE_NAME)

down:  ## Stops and removes all services and the network
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) down $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) down $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_MINIO_COMPOSE) down $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_NETWORK) rm $(NETWORK_NAME) $(ARGS_NETWORK)

clear:  ## Clears all services, network and volumes
	$(CMD_DOCKER_COMPOSE) $(FILE_SPARK_COMPOSE) down -v $(ARGS_COMPOSE)
	docker volume rm spark_logs
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_AIRFLOW_COMPOSE) down -v $(ARGS_COMPOSE)
	rm -rf $(PATH_AIRFLOW)/logs
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_COMPOSE) $(FILE_MINIO_COMPOSE) down -v $(ARGS_COMPOSE)
	@printf $(SPACE_BAR)
	$(CMD_DOCKER_NETWORK) rm $(NETWORK_NAME) $(ARGS_NETWORK)

get_logs: ## Copies the spark application logs from the spark-worker container to the host
	docker cp spark-worker:/opt/spark/logs/spark-app.log ./spark-app.log
	docker cp spark-worker:/opt/spark/logs/spark-run.log ./spark-run.log || true

help:  ## Shows this help
	@echo "Usage make [VAR=value] [targets]"
	@grep -E '^\S.*##' $(MAKFILE) | perl -pe 's/^(?:\.PHONY: |# )?([\.\w=<>]+):?.*  ## (.+)/  \1	\2/' | expand -t13 | sort

helpvars:  ## List common variables to override
	@grep -E '^\S.*#=' $(MAKFILE) | perl -pe 's/^(\w+).*#= (.+)/  \1	\2/' | expand -t13 | sort
