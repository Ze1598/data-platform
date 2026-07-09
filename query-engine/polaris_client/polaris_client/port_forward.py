import subprocess
import time
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def kubectl_port_forward(
    service: str, local_port: int, remote_port: int, namespace: str
) -> Iterator[None]:
    """Runs `kubectl port-forward` for the duration of the context, for
    scripts talking to an in-cluster service (like Polaris) from outside
    the cluster. Not needed by anything actually running as a pod in the
    same cluster — those can reach the service's in-cluster DNS name
    directly.
    """
    proc = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            "-n",
            namespace,
            f"svc/{service}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        time.sleep(3)
        if proc.poll() is not None:
            output = proc.stdout.read().decode() if proc.stdout else ""
            raise RuntimeError(f"kubectl port-forward exited early: {output}")
        yield
    finally:
        proc.terminate()
        proc.wait(timeout=10)
