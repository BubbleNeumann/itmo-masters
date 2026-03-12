import os
import uuid

SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/bitnami/spark")
JOBS_PATH = "/opt/jobs"


def submit_job(job_type: str, parameters: dict, main_class: str = None, jar_path: str = None):

    job_scripts = {
        'batch': f"{JOBS_PATH}/batch_etl.py",
        'streaming': f"{JOBS_PATH}/streaming_etl.py"
    }

    if job_type not in job_scripts:
        raise ValueError(f"Unknown job type: {job_type}")

    script_path = job_scripts[job_type]

    args = []
    for key, value in parameters.items():
        args.extend([f"--{key}", str(value)])

    cmd = [
              f"{SPARK_HOME}/bin/spark-submit",
              "--master", SPARK_MASTER,
              "--deploy-mode", "client",
              script_path
          ] + args

    if 'spark_conf' in parameters:
        for k, v in parameters['spark_conf'].items():
            cmd.extend(["--conf", f"{k}={v}"])

    submission_id = f"spark-{uuid.uuid4().hex[:8]}"

    # process = subprocess.Popen(
    #     cmd,
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE,
    #     text=True
    # )
    # #
    # job_info = {
    #     'submission_id': submission_id,
    #     'job_type': job_type,
    #     'parameters': parameters,
    #     'process': process,
    #     'start_time': time.time()
    # }

    return submission_id