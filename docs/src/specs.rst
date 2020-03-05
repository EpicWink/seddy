Workflows specifications
========================

Workflows specified in a workflows specs file can have different specification types.
The supported types follow:

.. toctree::
   :maxdepth: 1

   specs.dag

Specification
-------------

The workflow file has structure

* **version** (*string*): workflow specifications file version
* **workflows** (*array*): workflows' specifications

.. _common-spec:

Common specification
--------------------

A workflow (element of ``workflows``) has common specification

* **spec_type** (*string*): specification type
* **name** (*string*): workflow name
* **version** (*string*): workflow version
* **description** (*string*): optional, workflow description
* **registration** (*object*): optional, specifies workflow registration status default
  configuration, see `RegisterWorkflowType
  <https://docs.aws.amazon.com/amazonswf/latest/apireference/API_RegisterWorkflowType.html>`_

   * **active** (*boolean*): optional (default: true), intended workflow registration
     status (mark as false to deprecate a workflow)
   * **task_timeout** (*int or "NONE"*): optional, default decision task time-out
     (seconds), ``"NONE"`` for unlimited
   * **execution_timeout** (*int*): optional, default workflow execution time-out (seconds)
   * **task_list** (*string*): optional, default decision task-list, see `task lists
     <https://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dev-task-lists.html>`_
   * **task_priority** (*int*): optional, default decision task-list, see `setting task
     priority
     <https://docs.aws.amazon.com/amazonswf/latest/developerguide/programming-priority.html>`_
   * **child_policy** (*string*): optional, default decision task-list, see `child
     workflows
     <https://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dev-adv-child-workflows.html>`_
   * **lambda_role** (*string*): optional, default IAM role for Lambda access, see
     `using Lambda tasks
     <https://docs.aws.amazon.com/amazonswf/latest/developerguide/lambda-task.html#using-lambda-tasks-in-workflows>`_

Example
^^^^^^^

.. code-block:: yaml

   spec_type: test
   name: spam
   version: "1.0"
   description: A workflow with spam, spam, eggs and spam.
   registration:
     active: true
     task_timeout: 5
     execution_timeout: 3600
     task_list: coffee
     task_priority: 2
     child_policy: TERMINATE
     lambda_role: arn:aws:iam::spam:role/eggs
