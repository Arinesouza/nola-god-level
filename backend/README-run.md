1. Coloque esta pasta 'backend' dentro do repositório nola-god-level (ao lado do docker-compose.yml original).
2. Na raiz do repo (onde está o docker-compose.yml original), copie docker-compose.override.yml para a raiz também.
3. Execute:
   docker compose up --build

4. A API ficará disponível em http://localhost:8000
   - Health: http://localhost:8000/health
   - Docs:  http://localhost:8000/docs
