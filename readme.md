# Hermes: Autonomous AI Automation Agent

Hermes is an autonomous AI agent framework designed to perform file generation, web research, and network operations using local LLMs via Ollama. This project provides the core infrastructure to turn your local AI into a proactive developer and researcher.

## Project Structure
- `adapter.ts`: The bridge between the messaging platform (e.g., Fluxer) and the internal task queue.
- `worker.py`: The autonomous agent brain that manages workspaces, executes Python code, and handles internet/network operations.
- `docker-compose.yml`: Infrastructure orchestration for NATS and Redis.

## Prerequisites
- **Node.js** (v18+)
- **Python 3.10+** (with `venv`)
- **Docker & Docker Compose** (for message bus/state)
- **Ollama** (with Hermes 3 or similar model pulled)

## Build & Installation

### 1. Infrastructure
Start the message bus and state management services:
```bash
docker-compose up -d
```
## 2. Python Worker setup
```bash
python3 -m venv venv
source venv/bin/activate
```
```bash
pip install nats-py redis openai reportlab pandas matplotlib requests beautifulsoup4
```
```bash
export BOT_TOKEN="your_bot_token"
```

### 3. Adapter Setup 

```bash
# Install Node dependencies
npm install dotenv nats ioredis axios ws form-data uuid typescript @types/node tsx

# Run the adapter
export BOT_TOKEN="your_bot_token"
npx tsx adapter.ts
```

### 4. Running

In separate terminal windows (with activated environments):

Start the worker: python worker.py

Start the adapter: npx tsx adapter.ts

### 5. Configuration

Bot Token: Ensure BOT_TOKEN is set in your environment or a .env file.

Model: Ensure LOCAL_LLM_MODEL matches the model name in your local Ollama instance.

Workspaces: The agent automatically creates persistent directories in a ./workspaces/ folder based on user IDs.


