import asyncio
import json
import os
import base64
import sys
import re
from nats.aio.client import Client as NATS
import redis.asyncio as redis
from openai import AsyncOpenAI

# ==========================================
# CONFIGURATION
# ==========================================
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
LOCAL_LLM_URL = os.getenv("LOCAL_LLM_URL", "http://localhost:11434/v1")
LOCAL_LLM_MODEL = os.getenv("LOCAL_LLM_MODEL", "hermes3") 

client = AsyncOpenAI(base_url=LOCAL_LLM_URL, api_key="ollama")

async def run_worker():
    nc = NATS()
    await nc.connect(NATS_URL)
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    print(f"🟢 Web-Enabled Execution Worker connected to NATS and Redis")
    print(f"🧠 Pointing directly to Ollama API at: {LOCAL_LLM_URL}")

    async def message_handler(msg):
        data = json.loads(msg.data.decode())
        exec_id = data["metadata"]["execution_id"]
        channel_id = data["payload"]["channel_id"]
        user_id = data["payload"]["user_id"]
        original_prompt = data["payload"]["prompt"]
        
        print(f"\n📥 Picked up job {exec_id}. Prompt: '{original_prompt}'")
        
        workspace_dir = os.path.join(os.getcwd(), "workspaces", str(user_id))
        os.makedirs(workspace_dir, exist_ok=True)
        initial_files = set(os.listdir(workspace_dir))
        
        state = {
            "execution_id": exec_id,
            "fluxer_message_id": None,
            "current_state": "THINKING"
        }
        await r.setex(f"hermes:execution:{exec_id}", 300, json.dumps(state))

        async def update_state(text, is_final=False, attachments=None):
            payload = {
                "event": "state_transition",
                "metadata": {"execution_id": exec_id},
                "payload": {
                    "channel_id": channel_id,
                    "display_text": text,
                    "is_final": is_final,
                    "attachments": attachments or []
                }
            }
            await nc.publish("hermes.execution.state_change", json.dumps(payload).encode())

        await update_state("*⏳ Analyzing request and writing script...*")

        system_prompt = f"""You are an advanced Python automation agent with FULL access to the internet and the local network.
        You have a persistent workspace for this user located at: {workspace_dir}
        
        Capabilities:
        1. WEB SCRAPING: If the user asks you to look something up online, write a script using `requests` and `BeautifulSoup` to scrape the data, and explicitly `print()` the answer to the console.
        2. NETWORK OPS: If the user asks about network status or devices, use libraries like `socket` or run system pings via `subprocess`, and `print()` the results.
        3. FILE GENERATION: If generating PDFs, use `reportlab`. Save files directly to: {workspace_dir}
        
        CRITICAL RULES:
        - If you are answering a question via a script, you MUST `print()` the final answer so the user can see it.
        - ALWAYS wrap your python code in standard ```python ... ``` markdown blocks."""

        conversation_history = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": original_prompt}
        ]

        try:
            success = False
            final_answer = ""
            
            for attempt in range(1, 4): 
                response = await client.chat.completions.create(
                    model=LOCAL_LLM_MODEL,
                    messages=conversation_history,
                    temperature=0.2
                )
                
                ai_text = response.choices[0].message.content
                conversation_history.append({"role": "assistant", "content": ai_text})
                
                code_match = re.search(r'```python\n(.*?)\n```', ai_text, re.DOTALL)
                
                if code_match:
                    script_code = code_match.group(1)
                    script_path = os.path.join(workspace_dir, "generator.py")
                    
                    with open(script_path, "w") as f:
                        f.write(script_code)
                        
                    await update_state(f"*⚙️ Executing Python script... (Attempt {attempt}/3)*")
                    
                    # 1. Async PIP install using exact venv binary
                    pip_process = await asyncio.create_subprocess_exec(
                        sys.executable, "-m", "pip", "install", "-q", "reportlab", "pandas", "matplotlib", "requests", "beautifulsoup4",
                        cwd=workspace_dir, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await pip_process.communicate()
                    
                    # 2. Async Python execution using exact venv binary
                    script_process = await asyncio.create_subprocess_exec(
                        sys.executable, "generator.py",
                        cwd=workspace_dir, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await script_process.communicate()
                    
                    if script_process.returncode == 0:
                        success = True
                        stdout_output = stdout.decode('utf-8').strip()
                        if stdout_output:
                            final_answer = f"**Execution Output:**\n```text\n{stdout_output}\n```"
                        else:
                            final_answer = "✅ Successfully executed the script!"
                        break 
                    else:
                        error_msg = stderr.decode('utf-8')
                        print(f"⚠️ Script crashed on attempt {attempt}: {error_msg.strip()}")
                        await update_state(f"*🐛 Script crashed. Hermes is reading the error log and rewriting... (Attempt {attempt}/3)*")
                        
                        conversation_history.append({
                            "role": "user",
                            "content": f"The script crashed with this error:\n```\n{error_msg}\n```\nPlease fix the bug and output the complete, corrected python script."
                        })
                else:
                    final_answer = ai_text
                    success = True
                    break

            if not success:
                raise Exception("Failed to generate working code after 3 attempts.")

            final_files = set(os.listdir(workspace_dir))
            new_files = final_files - initial_files
            
            attachments = []
            for filename in new_files:
                filepath = os.path.join(workspace_dir, filename)
                if not filename.endswith('.py'): 
                    with open(filepath, "rb") as f:
                        encoded = base64.b64encode(f.read()).decode('utf-8')
                        attachments.append({
                            "filename": filename,
                            "data": encoded
                        })
                        print(f"📎 Picked up new file: {filename}")
            
            await update_state(final_answer, is_final=True, attachments=attachments)
            print(f"✅ Finished execution {exec_id}")
            
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            await update_state(f"*❌ Task failed: {str(e)}*", is_final=True)

    await nc.subscribe("hermes.worker.queue", queue="hermes_workers", cb=message_handler)
    
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(run_worker())
