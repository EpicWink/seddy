Data exchange in executions
===========================

.. seealso::

   `Data exchange SWF documentation
   <https://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dev-actors.html#swf-dev-actors-dataex>`_

SWF will always pass around strings for workflow and activity input and result, however
*seddy* will always JSON-deserialise it during processing. To get an arbitrary string as
input or result, simply provide a JSON string, eg ``"foo: bar"`` (ie include the
double-quotes in the string).

.. _dag-result:

DAG-type workflows result
-------------------------

The workflow result is built from the task results. Specifically, the task ID is used as
the key, the task's result as the value.

For example, a workflow with task IDs "task1", "task2", "task3" and "task4" could have
execution result:

.. code-block:: json

   {"task1": "eggs", "task3": null, "task4": {"c": [1, 2]}}

Note that a task won't have a corresponding entry in the workflow result if the task
doesn't provide a result.

.. _json-path:

Basic single-valued JSONPath
----------------------------

This is a subset of the JSONPath syntax, where only one value is to be retrieved, and no
functions are performed. The format rules are:

* must start with ``$``, for the root item
* object keys are prefixed by ``.``
* array indices are enclosed by ``[`` and ``]``
* child items are specified to the right

For example, suppose with the item

.. code-block:: json

   {"eggs": [{"spam": {"swallow": [null, null, 42]}}]}

Then the JSONPath ``$.eggs[0].spam.swallow[2]`` would give ``42``, and the JSONPath
``$.eggs[0].spam`` would give ``{"swallow": [null, null, 42]}``.
