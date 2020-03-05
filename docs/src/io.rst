Data exchange
=============

.. seealso::

   `SWF documentation <https://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dev-actors.html#swf-dev-actors-dataex>`_

*seddy* assumes all workflow input and output is JSON-serialisable, and will be
manipulated as such according to task IDs. The task ID is used as key for the task input
from the workflow input, and the task ID is used as the key to place the task's result
input the workflow result.

For example, a workflow with task IDs "task1", "task2", "task3" and "task4" could have
execution input:

.. code-block:: json

   {"task1": "spam", "task2": {"a": 42, "b": null}, "task4": null}

And execution result:

.. code-block:: json

   {"task1": "eggs", "task3": null, "task4": {"c": [1, 2]}}

Note that a task won't recieve input if it's not provided, and a task won't have a
corresponding entry in the workflow result if the task doesn't provide a result.

To get an arbitrary string as input or result, simply provide a JSON string, eg
``"foo: bar"``.