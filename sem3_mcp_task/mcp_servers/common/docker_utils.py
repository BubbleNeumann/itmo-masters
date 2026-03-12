import subprocess
import os

NAMENODE_CONTAINER = os.environ.get("HDFS_NAMENODE_CONTAINER", "namenode")  # Not used here, kept for compatibility

def run_docker_command(container: str, cmd: list, timeout: int = 30):
    full_cmd = ["docker", "exec", container] + cmd
    return subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout, check=False)