from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import time
from collections import deque
from threading import Lock
import heapq
import asyncio

CLASSIFICATION_SERVER_URL = "http://localhost:8001/classify"

app = FastAPI(
    title="Classification Proxy",
    description="Proxy server that handles batching and scheduling for the code classification service"
)

class ProxyRequest(BaseModel):
    sequence: str

class ProxyResponse(BaseModel):
    result: str

queue_lock = Lock()
short_request_queue = deque()
long_request_heap = []
batch_ready_sem = asyncio.Event() 
WAIT_CUTOFF = 12

async def batch_processor():
    while True:
        await batch_ready_sem.wait()
        with queue_lock:
            batch_ready_sem.clear()
            batch = []
            # simply empty out the 'smaller' queued-up requests before the larger one
            while short_request_queue and len(batch) < 5:
                seq = short_request_queue.popleft()
                batch.append(seq)
            while long_request_heap and len(batch) < 5:
                _, seq = heapq.heappop(long_request_heap)
                batch.append(seq)
        if batch:
            print(f"[INFO] Sending batch: {batch}")
            async with httpx.AsyncClient() as client:
                response = await client.post(CLASSIFICATION_SERVER_URL, json={"sequences": batch})
                if response.status_code == 200:
                    results = response.json()["results"]
                    print(f"[INFO] Batch processed, results: {results}")
                else:
                    print(f"[ERROR] Failed to process batch, status: {response.status_code}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(batch_processor())

@app.post("/proxy_classify")
async def proxy_classify(req: ProxyRequest):
    sequence = req.sequence
    with queue_lock:
        if len(sequence) <= WAIT_CUTOFF: 
            short_request_queue.append(sequence)
        else: # proabbly minimal benefit in trying to wait for smaller requets
            heapq.heappush(long_request_heap, (len(sequence), sequence))
        if len(short_request_queue) + len(long_request_heap) >= 5 or len(sequence) > WAIT_CUTOFF:
            batch_ready_sem.set()
    print(f"[INFO] Request queued: {sequence}")
    return {"message": "Request received."}
