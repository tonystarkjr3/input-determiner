import httpx
import asyncio

async def test_connectivity():
    async with httpx.AsyncClient() as client:
        try:
            payload = {"sequence": "test"}
            response = await client.post("http://localhost:8000/proxy_classify", json=payload)
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text}")
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(test_connectivity())