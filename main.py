from flask import Flask, request
from enums import Status, StatusEncoder
import json
import uuid
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

TIME_CRITICAL = "TIME_CRITICAL"
NOT_TIME_CRITICAL = "NOT_TIME_CRITICAL"

job_queue = {}


@app.route("/jobs/enqueue", methods=["POST"])
def enqueue():
    job_id = str(uuid.uuid4())
    logging.info(f"Enqueueing job with ID: {job_id}")

    time_critical = NOT_TIME_CRITICAL

    # This endpoint requires a body with a "time_critical" set to True or False
    try:
        request_data = request.get_json()
        if "time_critical" not in request_data:
            logging.error("KeyError: Request body must contain a 'time_critical' key")
            raise KeyError
        time_critical = (
            TIME_CRITICAL
            if request_data["time_critical"] == True
            else NOT_TIME_CRITICAL
        )
        logging.info(f"JOB ID: {job_id} is {time_critical}")
    except KeyError:
        return {
            "message": "KeyError: Request body must contain a 'time_critical' key",
        }, 400
    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "message": f"Error: {e}",
        }, 400

    # Notes/Assumptions:
    # - Type is NOT_TIME_CRITICAL by default unless a time_critical flag is passed
    # - Jobs are created in the QUEUED status
    job_queue[job_id] = {"ID": job_id, "Type": time_critical, "Status": Status.QUEUED}
    logging.info(f"Job with ID: {job_id} has been successfully enqueued")
    return json.dumps(job_queue[job_id]["ID"]), 200


@app.route("/jobs/dequeue", methods=["POST"])
def dequeue():
    # Returns a job from the queue
    # Jobs are considered available for Dequeue if the job
    # has not been concluded and has not dequeued already

    # The consumer will provide it's unique ID
    try:
        consumer_id = request.headers.get("QUEUE_CONSUMER")
        logging.info(f"Consumer ID: {consumer_id}")
        # In a real-world scenario, we would also validate that consumer_id is not None
    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "message": f"Error: {e}",
        }, 400

    if len(job_queue) == 0:
        logging.info("No jobs exist in the queue")
        return json.dumps({"message": "No jobs exist in the queue"}), 404
    else:
        for key, _ in job_queue.items():
            # Only dequeue jobs that are in the QUEUED status
            if job_queue[key]["Status"] == Status.QUEUED:
                job_queue[key]["Status"] = Status.IN_PROGRESS
                job_queue[key]["Consumer"] = consumer_id
                return json.dumps(job_queue[key], cls=StatusEncoder), 200
        return json.dumps({"message": "No jobs to dequeue"}), 404


@app.route("/jobs/<job_id>/conclude", methods=["POST"])
def conclude(job_id):
    # Provided an input of a job ID, finish execution on the job and consider it done
    consumer_id = request.headers.get("QUEUE_CONSUMER")

    if job_queue[job_id]:
        if job_queue[job_id]["Status"] == Status.IN_PROGRESS:
            job_queue[job_id]["Status"] = Status.CONCLUDED
            return json.dumps(job_queue[job_id], cls=StatusEncoder), 200
        else:
            return {"message": "Job is not currently in progress"}, 400

    # Check if the job is assigned to the consumer
    if job_queue[job_id]["Consumer"] != consumer_id:
        return {"error": "Consumer ID does not match header"}, 403

    return {"message": "Job not found"}, 404


@app.route("/jobs/<job_id>", methods=["GET"])
def get_info(job_id):
    return json.dumps(job_queue[job_id], cls=StatusEncoder), 200


if __name__ == "__main__":
    app.run(debug=True)

# With additional development time, I would create models for the job and requests objects
# and unpack the data into the models. This would allow for better validation and better
# maintainability.
