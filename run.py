from scheduler.job_manager import JobManager

patient_ids = ['17761']

jm = JobManager()
jm.process_batch(patient_ids)

failed = []
while not jm.error_queue.empty():
    failed.append(jm.error_queue.get())

print("FAILED:", failed)