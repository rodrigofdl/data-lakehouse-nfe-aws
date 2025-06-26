# Displays all available commands
help:
	@echo "Comandos disponíveis:"
	@echo "  make run            - Executa o pipeline localmente"
	@echo "  make build          - Constrói a imagem Docker"
	@echo "  make run-docker     - Executa o pipeline no Docker (sem Compose)"
	@echo "  make compose-up     - Sobe o ambiente com Docker Compose"
	@echo "  make compose-down   - Derruba os containers do Docker Compose"
	@echo "  make install        - Instala as dependências Python localmente"
	@echo "  make test           - Roda os testes localmente"
	@echo "  make clean-docker   - Limpa imagens/containers não utilizados"

# Install project dependencies
install:
	pip install -r requirements.txt

# Run the pipeline locally
run:
	python pipeline/main

# Builds the image Docker
build:
	docker build -t data-pipeline-nfe .

# Run the pipeline inside the container
run-docker:
	docker run --rm --env-file .env data-pipeline-nfe

# Climbs the complete environment with Compose
compose-up:
	docker compose up --build

# Overthrows the services of Compose
compose-down:
	docker compose down

# Local Testing Wheel 
test:
	pytest tests/

# Clean Docker resources
clean:
	docker system prune -f
	