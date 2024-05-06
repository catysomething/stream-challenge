from flask import Flask, request
from enums import Status, StatusEncoder
import json
import uuid

app = Flask(__name__)

TIME_CRITICAL = "TIME_CRITICAL"
NOT_TIME_CRITICAL = "NOT_TIME_CRITICAL"

job_queue = {}


# Define your endpoint
@app.route("/jobs/enqueue", methods=["POST"])
def enqueue():
    job_id = str(uuid.uuid4())
    time_critical = NOT_TIME_CRITICAL

    # This endpoint requires a body with a "time_critical" set to True or False
    try:
        request_data = request.get_json()
        if request_data:
            time_critical = (
                TIME_CRITICAL
                if request_data["time_critical"] == True
                else NOT_TIME_CRITICAL
            )
    # TODO: Create a class for custom error messages
    except:
        return {"message": "Request body must contain a 'time_critical' key"}, 400

    # Create the job object. Notes/Assumptions:
    # - Type is NOT_TIME_CRITICAL by default unless a time_critical flag is passed
    # - Jobs are created in the QUEUED status
    job_queue[job_id] = {"ID": job_id, "Type": time_critical, "Status": Status.QUEUED}

    return job_queue[job_id]["ID"], 200


@app.route("/jobs/dequeue", methods=["POST"])
def dequeue():
    # Returns a job from the queue
    # Jobs are considered available for Dequeue if the job
    # has not been concluded and has not dequeued already

    # TODO - Check to see if the job has been dequeued already
    # TODO - Check to see if the job has been concluded
    if len(job_queue) == 0:
        return json.dumps({"message": "No jobs exist in the queue"}), 404
    else:
        for key, _ in job_queue.items():
            print(key)
            if job_queue[key]["Status"] == Status.QUEUED:
                job_queue[key]["Status"] = Status.IN_PROGRESS
                return json.dumps(job_queue[key], cls=StatusEncoder), 200
        return json.dumps({"message": "No jobs to dequeue"}), 404


@app.route("/jobs/<job_id>/conclude", methods=["POST"])
def conclude(job_id):
    # Provided an input of a job ID, finish execution on the job and consider it done
    # TODO: Add a check to ensure job is in progress before concluding
    if job_queue[job_id]:
        job_queue[job_id]["Status"] = Status.CONCLUDED
        return json.dumps(job_queue[job_id], cls=StatusEncoder), 200
    return {"message": "Job not found"}, 404


@app.route("/jobs/<job_id>", methods=["GET"])
def get_info(job_id):
    return json.dumps(job_queue[job_id], cls=StatusEncoder), 200


if __name__ == "__main__":
    app.run(debug=True)
