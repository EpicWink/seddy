DAG-type workflow specification
===============================

A DAG (directed acyclic graph) workflow is a series of tasks that are scheduled to run
after their dependencies have finished. See :ref:`dag-result` for the result of a DAG
workflow.

Specification
-------------

A DAG-type workflow (element of ``workflows``) has specification

* **spec_type** (*string*): specification type, must be ``dag``
* **name**, **version**, **description** and **registration**: see :ref:`common-spec`
* **tasks** (*array[object]*): array of workflow activity tasks to be run during
  execution, see `ScheduleActivityTaskDecisionAttributes
  <https://docs.aws.amazon.com/amazonswf/latest/apireference/API_ScheduleActivityTaskDecisionAttributes.html>`_

   * **id** (*string*): task ID, must be unique within a workflow execution and without
     ``:``, ``/``, ``|``, ``arn`` or any control character
   * **type** (*object*): activity type, with **name** (*str*, activity name) and
     **version** (*str*, activity version)
   * **input** (*object*): activity input definition, see :ref:`dag-input`
   * **heartbeat** (*int or "NONE"*): optional, task heartbeat time-out (seconds), or
     ``"NONE"`` for unlimited
   * **timeout** (*int*): optional, task time-out (seconds), or ``"None"`` for unlimited
   * **task_list** (*string*): optional, task-list to schedule task on
   * **priority** (*int*): optional, task priority
   * **dependencies** (*array[string]*): optional, IDs of task's dependents
   * **skip_if** (*object*): optional, skip task if workflow state satisfies condition.
     See :ref:`dag-condition` for more details

.. _dag-input:

Input
-----

There are multiple options when defining activity task input. In the task input
specification (aka input-spec), **type** can have one of the following values:

* **none**: no value will be passed as input, meaning there will be no **input** key in
  the poll-for-activity-task response provided to the worker.

  .. code-block:: yaml

     input:
       type: none

* **constant**: the activity will be passed **value** in the input-spec, which can be
  any valid type.

  .. code-block:: yaml

     input:
       type: constant
       value: 42

  .. code-block:: yaml

     input:
       type: constant
       value:
         spam:
           - foo: bar
             eggs: 42
           - null
         swallow: false

* **workflow-input**: the activity will be passed a portion of the workflow input,
  according to **path** in the input-spec (see :ref:`json-path` for its syntax).
  **path** can be omitted, defaulting to ``"$"`` (the entire workflow input).
  Specify **default** to allow missing values, instead using the value of **default**

  .. code-block:: yaml

     input:
       type: workflow-input

  .. code-block:: yaml

     id: foo
     input:
       type: workflow-input
       path: $.foo

  .. code-block:: yaml

     input:
       type: workflow-input
       path: $.spam[0].eggs.swallow[2]

* **dependency-result**: the activity will be passed a portion of one of its
  dependencies' results, with the dependency acitivity task with ID **id** in the
  input-spec, according to **path** in the input-spec (see :ref:`json-path` for its
  syntax). **path** can be omitted, defaulting to ``"$"`` (the entire dependency
  result).
  Specify **default** to allow missing values, instead using the value of **default**

  .. code-block:: yaml

     dependencies:
       - foo
       - bar
     input:
       type: dependency-result
       id: bar

  .. code-block:: yaml

     dependencies:
       - foo
       - bar
     input:
       type: dependency-result
       id: bar
       path: $.swallow[2]

* **object**: you can have *seddy* build an object to be passed to the activity, with
  the value of each key being specified by its own input specification, as defined by
  **items** in the input-spec. This can be done recursively.

  .. code-block:: yaml

     dependencies:
       - foo
       - bar
     input:
       type: object
       items:
         spam:
           type: dependency-result
           id: foo
           path: $.swallow[2]
         eggs:
           type: object
           items:
             cheese:
               type: constant
               value: null
             pie:
               type: workflow-input
               path: $.spam[0].eggs.swallow[2]
             gravy:
               type: dependency-result
               id: bar
         ham:
           type: constant
           value: 42

Example
^^^^^^^

.. code-block:: yaml

   spec_type: dag
   name: spam
   version: "1.0"
   description: A workflow with spam, spam, eggs and spam.
   registration:
     active: true
     task_timeout: 5
     execution_timeout: 3600
     task_list: coffee
   tasks:
     - id: foo
       type:
         name: spam-foo
         version: "0.3"
       input:
         type: workflow-input
         value: $.foo
       timeout: 10
       task_list: eggs
       priority: 1
     - id: bar
       type:
         name: spam-foo
         version: "0.4"
       input:
         type: constant
         value: 42
       timeout: 10
       task_list: eggs
       dependencies:
       - foo


.. _dag-condition:

Condition
---------

Condition objects represent a logical expression, and their form depends on the value of
their **type**, which can be one of the following:

* **=**, **!=**, **<**, **<=**, **in**: (Pythonic) value comparison, with sibling'
  properties:

  * **lhs** (*object*): left-hand-side value specification, as defined in
    :ref:`dag-input`
  * **rhs** (*object*): right-hand-side value specification, as defined in
    :ref:`dag-input`

  .. code-block:: yaml

     type: '='
     lhs:
       type: workflow-input
       path: $.runSpam
     rhs:
       type: constant
       value: false

  .. code-block:: yaml

     type: in
     lhs:
       type: dependency-result
       id: bar
       path: $.foo
     rhs:
       type: workflow-input
       path: $.skipFoos

* **and**, **or**: logical combinator, with sibling properties:

  * **lhs** (*object*): left-hand-side condition object
  * **rhs** (*object*): right-hand-side condition object

  .. code-block:: yaml

     type: or
     lhs:
       type: '!='
       lhs:
         type: const
         value: true
       rhs:
         type: const
         value: false
     rhs:
       type: and
       lhs:
         type: in
         lhs:
           type: const
           value: oof
         rhs:
           type: const
           value: foobar
       rhs:
         type: in
         lhs:
           type: const
           value: foo
         rhs:
           type: const
           value:
             - spam
             - eggs

* **not**: logical not expression, with notted expression specified as a condition
  object in **value**.

  .. code-block:: yaml

     type: not
     value:
       type: '='
       lhs:
         type: const
         value: true
       rhs:
         type: const
         value: true
