import asyncio
import time
import httpx

probes = [
    "привет!",
    "Топ 5 расходов за 2024",
    "какая общая сумма переводов?",
    "дай список всех людей с кем были переводы"
]

async def main():
    async with httpx.AsyncClient(timeout=120) as client:
        # Before was approx 15 seconds average due to multiple synchronous steps and unoptimized summarize.
        # Now let's test stream endpoint time to first chunk and total time.
        
        for q in probes:
            print(f"Testing: {q}")
            start = time.time()
            first_chunk_time = None
            total_response_len = 0
            async with client.stream("POST", "http://localhost:8000/api/v1/chat/stream", json={"question": q}) as response:
                async for chunk in response.aiter_text():
                    if not first_chunk_time:
                        first_chunk_time = time.time()
                    total_response_len += len(chunk)
            
            end = time.time()
            if first_chunk_time:
                print(f"  TTFB (Time to First Byte): {first_chunk_time - start:.2f}s")
                print(f"  Total Time: {end - start:.2f}s")
            else:
                print("  Failed to get response")
            print()

if __name__ == "__main__":
    asyncio.run(main())