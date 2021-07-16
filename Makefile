all: server

server:
	uvicorn app:app

deps:
	pip3 install -r requirements.txt
	docker-compose up -d
