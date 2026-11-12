"""Container jobs (parser, Spark clean) that Airflow retries only when Docker killed them for memory."""

try:
    from airflow.providers.docker.exceptions import DockerContainerFailedException
    from airflow.providers.docker.operators.docker import DockerOperator
    from airflow.sdk.exceptions import AirflowFailException
    from docker.errors import APIError
except ImportError:  # host tests run without Airflow and use only should_retry
    DockerContainerFailedException = AirflowFailException = APIError = Exception
    DockerOperator = object

EXIT_CODE_KEY = "exit_code"


def should_retry(state: dict) -> bool:
    """True when Docker's container State shows a kill for memory (OOMKilled or exit 137)."""
    return bool(state.get("OOMKilled")) or state.get("ExitCode") == 137


class JobOperator(DockerOperator):
    """DockerOperator that fails without retrying unless should_retry holds for the exited
    container. The container is always removed once its state has been read."""

    def __init__(self, **kwargs):
        # the provider's auto_remove deletes the container before a failure reaches execute,
        # leaving nothing to inspect, so this class removes it itself
        super().__init__(auto_remove="never", **kwargs)

    def execute(self, context):
        try:
            return super().execute(context)
        except DockerContainerFailedException as e:
            state = self._exit_state()
            if "ExitCode" in state:
                # the sweeps count only a container that ran and exited, not one that never started
                context["ti"].xcom_push(key=EXIT_CODE_KEY, value=state["ExitCode"])
            if should_retry(state):
                raise
            raise AirflowFailException(
                f"container exited with code {state.get('ExitCode', 'unknown')}, not retried") from e
        finally:
            self._remove_container()

    def _exit_state(self) -> dict:
        try:
            return self.cli.inspect_container(self.container["Id"])["State"]
        except Exception as e:  # an unknown state counts as not retryable
            self.log.warning("could not inspect the container: %s", e)
            return {}

    def _remove_container(self) -> None:
        if self.container is None:
            return
        try:
            self.cli.remove_container(self.container["Id"])
        except APIError as e:
            self.log.warning("could not remove container %s: %s", self.container["Id"], e)
