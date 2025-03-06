from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import time
from collections import deque
from threading import Lock

CLASSIFICATION_SERVER_URL = "http://localhost:8001/classify"

app = FastAPI(
    title="Classification Proxy",
    description="Proxy server that handles rate limiting and retries for the code classification service"
)

class ProxyRequest(BaseModel):
    """Request model for single text classification"""
    sequence: str

class ProxyResponse(BaseModel):
    """Response model containing classification result ('code' or 'not code')"""
    result: str


request_queue = deque()
response_dict = {}
proxy_lock = Lock()

@app.post("/proxy_classify")
def proxy_classify(req: ProxyRequest):
    """
    Proxies a single text sequence to the classification server.
    Handles rate limiting by implementing a retry mechanism when the server is busy.

    TODO: Implement your own batching and queueing logic here
    """
    global request_queue, response_dict
    
    # Add new request to queue
    request_queue.append(req.sequence)
    
    # Try to acquire lock for processing
    lock_acquired = proxy_lock.acquire(blocking=False)
    if not lock_acquired:
        # If we can't get the lock, wait for our request to be processed
        while req.sequence in request_queue:
            time.sleep(0.1)
        # Get our result from the response dict
        if req.sequence in response_dict:
            result = response_dict.pop(req.sequence)
            return ProxyResponse(result=result)

    
    try:
        with httpx.Client() as client:
            while request_queue:
                sequence = request_queue[0]
                while True:  # Keep trying until we get a successful response
                    try:
                        classification_request = {"sequences": [sequence]}
                        response = client.post(CLASSIFICATION_SERVER_URL, json=classification_request)
                        
                        if response.status_code == 200:
                            data = response.json()
                            request_queue.popleft()  # Remove processed request
                            result = data["results"][0]
                            if sequence == req.sequence:
                                return ProxyResponse(result=result)
                            else:
                                response_dict[sequence] = result
                            break  # Got a successful response, move to next request
                        elif response.status_code == 429:
                            time.sleep(0.1)  # Wait before retry
                        else:
                            response.raise_for_status()
                    except Exception:
                        time.sleep(0.1)  # Wait before retry on any error
    finally:
        if lock_acquired:
            proxy_lock.release()
