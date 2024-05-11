from concurrent.futures import  ThreadPoolExecutor, as_completed
import json
import queue
import random
import threading
import time
import traceback
from fastapi import FastAPI, Response
import uvicorn

app = FastAPI()

MAX_CONCURRENT_TASK = 2
TASK_QUEUE = queue.Queue(10)


def solve(id):
    time.sleep(id)
    return f"COMPLETED {id}"

def simulate_long_running_task():
    try:
        exec_time = [random.randrange(8, 10) for _ in range(3)]
        print("Started", id,exec_time)
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(solve, id) for id in exec_time]
            for future in as_completed(futures):
                print(future.result())

        print("Finished", id)
    except Exception as e:
        traceback.print_exc()


def worker():
    print("Worker running...")
    while True:
        print("TOTAL THREADs",len(threading.enumerate()))
        tasks = []
        for _ in range(MAX_CONCURRENT_TASK):
            if not TASK_QUEUE.empty():
                id = TASK_QUEUE.get()
                print(f"Processing task {id}")
                task_thread = threading.Thread(target=simulate_long_running_task)
                task_thread.start()
                tasks.append(task_thread)
            else:
                break
        for task in tasks:
            task.join()
        if TASK_QUEUE.empty():
            print("No tasks in the queue")
            time.sleep(1)


@app.get("/check")
def get_health_check():
    response = {
        "data": None,
        "message": "Server is working",
        "status_code": 200
    }
    return Response(json.dumps(response), 200)


@app.get("/add-task")
def add_task_to_queue(id: int):
    TASK_QUEUE.put(id)
    print(f"Task {id} added to queue")
    response = {
        "data": None,
        "message": f"Added id {id} to task queue",
        "status_code": 200
    }
    return Response(json.dumps(response), 200)


@app.get("/tasks")
def get_tasks_in_queue():
    response = {
        "data": {
            "tasks_in_queue": TASK_QUEUE.qsize()
        },
        "message": "Retrieved tasks in the queue",
        "status_code": 200
    }
    return Response(json.dumps(response), 200)


if __name__ == "__main__":
    threading.Thread(target=worker, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
