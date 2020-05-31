"""SWF decisions making."""

import abc
import enum
import dataclasses
import typing as t


class DeciderError(RuntimeError):
    """Misconfiguration of the decider."""


class ChildPolicy(enum.Enum):
    """Policy for child executions on parent termination.

    .. seealso::
        `StartWorkflowExecution in SWF API documentation
        <https://docs.aws.amazon.com/amazonswf/latest/apireference/API_StartWorkflowExecution.html#SWF-StartWorkflowExecution-request-childPolicy>`_
    """

    TERMINATE = "TERMINATE"
    REQUEST_CANCEL = "REQUEST_CANCEL"
    ABANDON = "ABANDON"


@dataclasses.dataclass
class Registration:
    """Workflow registration configuration.

    Args:
        active: registration status, ``False`` for deprecated
        task_timeout: default decision task time-out (seconds), or "NONE"
            for unlimited
        execution_timeout: default workflow execution time-out (seconds)
        task_list: default decision task-list
        task_priority: default decision task priority
        child_policy: default policy for child executions upon parent
            execution termination
        lambda_role: default IAM role for Lambda access
    """

    active: bool = True
    task_timeout: t.Union[int, str] = None
    execution_timeout: int = None
    task_list: str = None
    task_priority: int = None
    child_policy: ChildPolicy = None
    lambda_role: str = None

    @classmethod
    def from_spec(cls, spec: t.Dict[str, t.Any]):
        """Construct registration configuration from specification.

        Args:
            spec: workflow registration configuration specification
        """

        kw = {}
        if "active" in spec:
            kw["active"] = spec["active"]
        if "task_timeout" in spec:
            kw["task_timeout"] = spec["task_timeout"]
        if "execution_timeout" in spec:
            kw["execution_timeout"] = spec["execution_timeout"]
        if "task_list" in spec:
            kw["task_list"] = spec["task_list"]
        if "task_priority" in spec:
            kw["task_priority"] = spec["task_priority"]
        if "child_policy" in spec:
            kw["child_policy"] = ChildPolicy(spec["child_policy"])
        if "lambda_role" in spec:
            kw["lambda_role"] = spec["lambda_role"]
        return cls(**kw)


class DecisionsBuilder(metaclass=abc.ABCMeta):
    """SWF decision builder.

    Args:
        workflow: workflow specification
        task: decision task
    """

    def __init__(self, workflow: "Workflow", task: t.Dict[str, t.Any]):
        self.workflow = workflow
        self.task = task
        self.decisions = []

    @abc.abstractmethod
    def build_decisions(self):  # pragma: no cover
        """Build decisions from workflow history."""
        raise NotImplementedError


class Workflow(metaclass=abc.ABCMeta):
    """SWF workflow specification.

    Args:
        name: workflow name
        version: workflow version
        registration: workflow registration configuration
    """

    _registration_cls = Registration

    def __init__(
        self,
        name: str,
        version: str,
        description: str = None,
        registration: Registration = None,
    ):
        self.name = name
        self.version = version
        self.description = description
        self.registration = registration

    @classmethod
    def _args_from_spec(
        cls, spec: t.Dict[str, t.Any]
    ) -> t.Tuple[tuple, t.Dict[str, t.Any]]:
        """Construct initialisation arguments from workflow specification.

        Args:
            spec: workflow specification

        Returns:
            initialisation positional and keyword arguments
        """

        args = (spec["name"], spec["version"])
        kwargs = {}
        if "description" in spec:
            kwargs["description"] = spec["description"]
        if "registration" in spec:
            kwargs["registration"] = cls._registration_cls.from_spec(
                spec["registration"]
            )
        return args, kwargs

    @classmethod
    def from_spec(cls, spec: t.Dict[str, t.Any]):
        """Construct workflow type from specification.

        Args:
            spec: workflow specification
        """

        args, kwargs = cls._args_from_spec(spec)
        return cls(*args, **kwargs)

    @property
    @abc.abstractmethod
    def decisions_builder(self) -> DecisionsBuilder:  # pragma: no cover
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def spec_type(self) -> str:  # pragma: no cover
        raise NotImplementedError

    def setup(self):
        """Set up workflow specification.

        Useful for pre-calculation or other initialisation.
        """

    def make_decisions(self, task: t.Dict[str, t.Any]) -> t.List[t.Dict[str, t.Any]]:
        """Build decisions from workflow history.

        Args:
            task: decision task

        Returns:
            workflow decisions
        """

        builder = self.decisions_builder(self, task)
        builder.build_decisions()
        return builder.decisions


def make_decisions_on_error(exception: Exception) -> t.List[t.Dict[str, t.Any]]:
    """Build workflow-fail decision on decider exception.

    Args:
        exception: decider exception being handled

    Returns:
        workflow-fail decision
    """

    decision_attrs = {"reason": exception.__class__.__name__}
    message = str(exception)
    if message:
        decision_attrs["details"] = message
    decision = {
        "decisionType": "FailWorkflowExecution",
        "failWorkflowExecutionDecisionAttributes": decision_attrs,
    }
    return [decision]
