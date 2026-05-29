from datetime import datetime
import uuid
import time

def generate_datetime():
    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return generated_date
    
def generate_run_id():
    unique_id = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    run_id = f"Run_{timestamp}_{unique_id}"
    return run_id
