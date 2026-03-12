import time

running_jobs = {}


def register_job(submission_id, job_info):
    running_jobs[submission_id] = job_info


def get_status(submission_id: str) -> dict:
    if submission_id not in running_jobs:
        return {
            'submission_id': submission_id,
            'status': 'UNKNOWN',
            'error': 'Job not found'
        }

    job_info = running_jobs[submission_id]
    process = job_info.get('process')

    if not process:
        return {'submission_id': submission_id, 'status': 'UNKNOWN'}

    poll = process.poll()

    if poll is None:
        # still running
        return {
            'submission_id': submission_id,
            'status': 'RUNNING',
            'elapsed': time.time() - job_info['start_time']
        }
    elif poll == 0:
        return {
            'submission_id': submission_id,
            'status': 'COMPLETED',
            'exit_code': poll,
            'elapsed': time.time() - job_info['start_time']
        }
    else:  # fail
        stderr = process.stderr.read() if process.stderr else 'Unknown error'
        return {
            'submission_id': submission_id,
            'status': 'FAILED',
            'exit_code': poll,
            'error': stderr
        }


def kill_job(submission_id: str) -> dict:
    """Kill a running Spark job"""
    if submission_id not in running_jobs:
        return {'submission_id': submission_id, 'status': 'NOT_FOUND'}

    job_info = running_jobs[submission_id]
    process = job_info.get('process')

    if process and process.poll() is None:
        process.terminate()
        return {'submission_id': submission_id, 'status': 'TERMINATED'}

    return {'submission_id': submission_id, 'status': 'NOT_RUNNING'}


def get_logs(submission_id: str, lines: int = 100) -> str:
    """Get logs from Spark job"""
    if submission_id not in running_jobs:
        return f"No logs found for {submission_id}"

    job_info = running_jobs[submission_id]
    process = job_info.get('process')

    if not process:
        return "No process found"

    # Read stdout/stderr
    stdout = process.stdout.read() if process.stdout else ''
    stderr = process.stderr.read() if process.stderr else ''

    logs = f"STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"

    # Truncate if needed
    if len(logs) > lines * 100:  # Rough estimate
        logs = logs[:lines * 100] + "\n... (truncated)"

    return logs