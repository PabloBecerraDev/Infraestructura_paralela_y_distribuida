# garch-intraday-strategy
A project for the 'Infraestructuras Paralelas y Distribuidas' class, using FastAPI, Ray Serve, and Docker Compose.
It implements an intraday trading strategy using a GARCH(1,3) model for volatility forecasting, and distributes its components into microservices.
How to run

    Clone or download the repository.

    Then, in the root of the project (where the docker-compose.yml is located), open a terminal and run:
    
    docker-compose build
    docker-compose up

Endpoints to execute
1. http://localhost:8000/daily_data
2. http://localhost:8000/intraday_data	
3. http://localhost:8001/health
4. http://localhost:8001/GARCHService
5. http://localhost:8002/generate_signals
6. http://localhost:8003/calculate (POST)
7. http://localhost:8004/run_strategy