# Variables
VENV := env
PYTHON := $(VENV)/Scripts/python
PIP := $(VENV)/Scripts/pip

# Default target
.DEFAULT_GOAL := help

# Help command: Shows available tasks
help:
	@echo "Available commands:"
	@echo "  make venv            - Create virtual environment"
	@echo "  make install         - Install project dependencies"
	@echo "  make run             - Run the ETL pipeline"
	@echo "  make test            - Run all unit tests"
	@echo "  make lint            - Run code formatting checks"
	@echo "  make clean           - Remove temporary files"
	@echo "  make creds           - Set Google Cloud credentials"

# Create Virtual Environment
venv:
	@echo "Creating virtual environment..."
	python -m venv $(VENV)
	@echo "Virtual environment created."

# Install Dependencies
install: venv
	@echo "Activating virtual environment and installing dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Dependencies installed."

# Run the ETL Pipeline
run:
	@echo "Running ETL pipeline..."
	$(PYTHON) src/pipeline.py
	@echo "ETL pipeline executed successfully."

# Run Tests
test:
	@echo "Running unit tests..."
	$(PYTHON) -m pytest tests/
	@echo "Tests completed."

# Lint & Format Code (PEP8, Black)
lint:
	@echo "Checking and formatting code..."
	$(PYTHON) -m black src/ tests/
	$(PYTHON) -m flake8 src/ tests/
	@echo "Linting completed."

# Remove temporary files
clean:
	@echo "Cleaning up temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf $(VENV)
	rm -rf logs/*
	rm -rf .pytest_cache
	@echo "Cleanup completed."

#  Set Google Cloud Credentials
creds:
	@echo "Setting Google Cloud credentials..."
	export GOOGLE_APPLICATION_CREDENTIALS=$$(grep GOOGLE_APPLICATION_CREDENTIALS .env | cut -d '=' -f2)
	@echo "Google Cloud credentials set."
