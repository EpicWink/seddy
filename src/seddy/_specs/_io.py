"""Workflows specs serialisation and desieralisation."""

import json
import base64
import pathlib
import dataclasses
import typing as t
import logging as lg
import urllib.parse

from .. import _util
from . import Workflow

if t.TYPE_CHECKING:
    import botocore.client

logger = lg.getLogger(__package__)


class WorkflowNotFound(LookupError):
    """Workflow not found."""


def _construct_workflows(
    workflow_specs: t.List[t.Dict[str, t.Any]],
) -> t.List[Workflow]:
    """Construct workflows from specification.

    Args:
        workflow_specs: workflows specifications

    Returns:
        workflow type specifications
    """

    from . import WORKFLOW

    workflows = []
    for workflow_spec in workflow_specs:
        workflow_cls = WORKFLOW[workflow_spec["spec_type"]]
        workflow = workflow_cls.from_spec(workflow_spec)
        workflows.append(workflow)
    return workflows


def _load_specs(workflows_file: pathlib.Path) -> t.Dict[str, t.Any]:
    """Load workflows specifications file.

    Determines load method from the file suffix. Supported file types:

    * JSON
    * YAML

    Args:
        workflows_file: workflows specifications file path

    Returns:
        workflows specifications
    """

    logger.info("Loading workflows specifictions from '%s'", workflows_file)
    workflows_text = workflows_file.read_text()
    if workflows_file.suffix == ".json":
        return json.loads(workflows_text)
    elif workflows_file.suffix in (".yml", ".yaml"):
        try:
            import yaml
        except ImportError as e:
            try:
                from ruamel import yaml
            except ImportError as er:
                raise e from er
        return yaml.safe_load(workflows_text)
    raise ValueError("Unknown extension: %s" % workflows_file.suffix)


def _from_dynamodb(item: t.Dict[str, t.Any]) -> t.Any:  # TODO: unit-test
    def float_or_int(v: str) -> t.Union[int, float]:
        try:
            return int(v)
        except ValueError:
            return float(v)

    (type_code,) = item.keys()
    value = item[type_code]
    if type_code == "M":
        return {k: _from_dynamodb(v) for k, v in value.items()}
    elif type_code == "L":
        return [_from_dynamodb(v) for v in value]
    elif type_code == "SS":
        return {v for v in value}
    elif type_code == "BS":
        return {base64.b64decode(v) for v in value}
    elif type_code == "NS":
        return {float_or_int(v) for v in value}
    elif type_code == "S":
        return value
    elif type_code == "B":
        return base64.b64decode(value)
    elif type_code == "N":
        return float_or_int(value)
    elif type_code == "BOOL":
        return value
    elif type_code == "NULL":
        return None
    else:
        raise ValueError("Unknown DynamoDB type-code '%s': %s" % (type_code, item))


@dataclasses.dataclass
class _DynamoDBSpecs:  # TODO: unit-test
    """DynamoDB workflow specifications interface."""

    client: "botocore.client.BaseClient"
    table_name: str

    @classmethod
    def from_uri(cls, uri_parts: urllib.parse.ParseResult) -> "_DynamoDBSpecs":
        """Get DynamoDB interfacing client from URI.

        Args:
            uri_parts: workflows specifications (URL-parsed parts of) URI

        Returns:
            DynamoDB workflow specifications interface
        """

        import boto3

        region = uri_parts.hostname or None
        table_name = uri_parts.path[1:]
        query_parameters = urllib.parse.parse_qsl(uri_parts.query)
        endpoint = next((v for k, v in query_parameters if k == "endpointUrl"), None)
        client = boto3.client("dynamodb", region_name=region, endpoint_url=endpoint)
        return cls(client, table_name)

    def _get_kwargs(self) -> t.Dict[str, t.Any]:
        return dict(
            TableName=self.table_name,
            ProjectionExpression="#s, #n, #v, #d, #r, #t",
            ExpressionAttributeNames={
                "#s": "spec_type",
                "#n": "name",
                "#v": "version",
                "#d": "description",
                "#r": "registration",
                "#t": "tasks",
            },
        )

    def get_all(self) -> t.List[t.Dict[str, t.Any]]:
        """Get all workflows.

        Returns:
            workflows specification
        """

        response = _util.list_paginated(
            self.client.scan,
            list_key="Items",
            kwargs=self._get_kwargs(),
            next_key="LastEvaluatedKey",
            next_arg="ExclusiveStartKey",
        )
        items = response.get("Items") or []
        return [_from_dynamodb(v) for v in items]

    def get(self, name: str, version: str) -> t.Dict[str, t.Any]:
        """Get a workflow.

        Args:
            name: workflow name
            version: workflow version

        Returns:
            requested workflow specification

        Raises:
            WorkflowNotFound: if workflow with given name and version not found
        """

        key = {"name_version": {"S": "%s-%s" % (name, version)}}
        kwargs = self._get_kwargs()
        response = self.client.get_item(Key=key, **kwargs)
        if not response.get("Item"):
            raise WorkflowNotFound("name=%s, version=%s" % (name, version))
        return _from_dynamodb(response["Item"])


def _load_specs_from_uri(  # TODO: unit-test
    workflows_spec_uri: str
) -> t.List[t.Dict[str, t.Any]]:
    """Load workflows specifications.

    Supported URI types:

    * file:// (or none): local file path (absolute or relative)

    Args:
        workflows_spec_uri: workflows specifications file URI

    Returns:
        workflows specifications
    """

    logger.debug("Workflows specifications URI '%s'", workflows_spec_uri)
    url_parts = urllib.parse.urlparse(workflows_spec_uri)
    if not url_parts.scheme or url_parts.scheme == "file":
        if url_parts.scheme == "file":
            workflows_spec_uri = workflows_spec_uri[7:]
        workflows_spec = _load_specs(pathlib.Path(workflows_spec_uri))
        if not (1,) < tuple(map(int, workflows_spec["version"].split("."))) < (2,):
            raise ValueError(
                "Unknown workflows file version: %s" % workflows_spec["version"]
            )
        return workflows_spec["workflows"]
    elif url_parts.scheme == "dynamodb":
        ddb_interface = _DynamoDBSpecs.from_uri(url_parts)
        return ddb_interface.get_all()
    else:
        raise ValueError(
            "Unknown URI scheme '%s': %s" % (url_parts.scheme, workflows_spec_uri)
        )


def load_workflows(workflows_spec_uri: str) -> t.List[Workflow]:
    """Load workflows specifications.

    Determines load method from the file suffix. Supported file types:

    * JSON
    * YAML

    Supported URI types:

    * file:// (or none): local file path (absolute or relative)
    * ``dynamodb://[region]/<table-name>[?endpointUrl=<endpoint-url>]``:
      DynamoDB-stored, with optional specified region or endpoint-URL

    Args:
        workflows_spec_uri: workflows specifications URI

    Returns:
        workflows specifications
    """

    workflows_specs = _load_specs_from_uri(workflows_spec_uri)
    return _construct_workflows(workflows_specs)


def get_workflow(
    name: str,
    version: str,
    workflows_spec_uri: str,
) -> Workflow:
    """Get workflow specification.

    Args:
        name: workflow name
        version: workflow version
        workflows_spec_uri: workflows specifications URI

    Returns:
        workflow specification

    Raises:
        WorkflowNotFound: if workflow with given name and version not found
    """

    url_parts = urllib.parse.urlparse(workflows_spec_uri)
    if url_parts.scheme == "dynamodb":
        ddb_interface = _DynamoDBSpecs.from_uri(url_parts)
        workflow_spec = ddb_interface.get(name, version)
        (workflow,) = _construct_workflows([workflow_spec])
        return workflow

    workflows = load_workflows(workflows_spec_uri)
    try:
        return next(w for w in workflows if w.name == name and w.version == version)
    except StopIteration:
        raise WorkflowNotFound("name=%s, version=%s" % (name, version)) from None
