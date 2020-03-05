DAG-type workflow specification
===============================

A DAG (directed acyclic graph) workflow is a series of tasks that are scheduled to run
after their dependencies have finished.

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
   * **type**: activity type, with **name** (*str*, activity name) and **version**
     (*str*, activity version)
   * **heartbeat** (*int or "NONE"*): optional, task heartbeat time-out (seconds), or
     ``"NONE"`` for unlimited
   * **timeout** (*int*): optional, task time-out (seconds), or ``"None"`` for unlimited
   * **task_list** (*string*): optional, task-list to schedule task on
   * **priority** (*int*): optional, task priority
   * **dependencies** (*array[string]*): optional, IDs of task's dependents

Example
-------

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
       timeout: 10
       task_list: eggs
       priority: 1
     - id: bar
       type:
         name: spam-foo
         version: "0.4"
       timeout: 10
       task_list: eggs
       dependencies:
       - foo
