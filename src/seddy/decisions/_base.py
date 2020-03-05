"""SWF decisions making."""

import abc
import typing as t


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
    """

    def __init__(self, name: str, version: str, description: str = None):
        self.name = name
        self.version = version
        self.description = description

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
