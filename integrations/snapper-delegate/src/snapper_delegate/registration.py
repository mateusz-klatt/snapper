"""Register the dependency-light consult delegate for managed execution."""

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import DelegateProcessParameters
from snapper.application.process_manager.registry import register_process
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper_delegate.runner import DelegateRunner


@register_process(
    name="delegate_runner",
    description="Generic managed delegate workload",
    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    role=ProcessRoleEnum.CORE,
    tags=("delegate", "runner"),
    parameters_model=DelegateProcessParameters,
    enabled=False,
    mode=ProcessModeEnum.PROCESS,
)
class ManagedDelegateRunner(DelegateRunner):
    """Expose the consult delegate through Snapper's existing process registry."""

    @staticmethod
    def get_default_parameters(settings: object) -> dict[str, object]:
        """Return no unsafe implicit defaults for a managed delegate template."""
        del settings
        return {}


RegisterableProcess.register(ManagedDelegateRunner)
