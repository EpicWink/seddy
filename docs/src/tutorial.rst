SWF decider tutorial
====================

Running an SWF decider for a virtual AWS.

We'll use `moto <https://github.com/spulec/moto>`_, a tool which mocks out SWF.

.. warning::

   ``moto`` v1.3.16 is required to correctly mock SWF (however, unlike using AWS for
   real, task-polling returns instantly).

Set-up
------

Install `Moto <http://docs.getmoto.org/en/latest/>`_,
`AWS CLI <https://aws.amazon.com/cli/>`_,
`PyYaml <https://github.com/yaml/pyyaml>`_ and seddy

.. code-block:: shell

   pip install moto[server,swf] pyyaml seddy

.. _env-vars:

You are free to use whichever method you like to install AWS CLI, for example installing
v1 via pip (``pip install awscli``) or using the Docker image (``docker pull
amazon/aws-cli:latest``, then ``alias aws='docker run --rm amazon/aws-cli:latest'``)

Environment variables
^^^^^^^^^^^^^^^^^^^^^

To use ``moto``, we need to point the AWS CLI and ``seddy`` to its server (which we'll
start below)

.. code-block:: shell

   export AWS_DEFAULT_REGION=us-east-1
   export AWS_SWF_ENDPOINT_URL=http://localhost:5042/

Example
-------

Create workflow definitions file

.. raw:: html

   <details>
   <summary><a>workflows.yml</a></summary>

.. include:: workflows.yml
   :code: yaml

.. raw:: html

   </details>

----

Start the mock SWF server (in a separate terminal: don't forget :ref:`env-vars`)

.. code-block:: shell

   moto_server swf -p5042

----

Register domain

.. code-block:: shell

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf register-domain \
     --name test-domain --workflow-execution-retention-period-in-days 2

----

Register defined workflows with SWF

.. code-block:: shell

   seddy -v register workflows.yml test-domain

----

Register referenced activities with SWF

.. code-block:: shell

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf register-activity-type \
     --domain test-domain \
     --name spam-foo \
     --activity-version 0.3 \
     --default-task-start-to-close-timeout 20 \
     --default-task-schedule-to-start-timeout 600 \
     --default-task-schedule-to-close-timeout 620 \
     --default-task-heartbeat-timeout 20 \
     --default-task-list name=test-activity-list

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf register-activity-type \
     --domain test-domain \
     --name spam-foo \
     --activity-version 0.4 \
     --default-task-start-to-close-timeout 20 \
     --default-task-schedule-to-start-timeout 600 \
     --default-task-schedule-to-close-timeout 620 \
     --default-task-heartbeat-timeout 20 \
     --default-task-list name=test-activity-list

----

Start the decider (in a separate terminal: don't forget :ref:`env-vars`)

.. code-block:: shell

   seddy -v decider workflows.yml test-domain test-list

----

Start a workflow execution

.. code-block:: shell

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf start-workflow-execution \
     --domain test-domain \
     --workflow-id test-wf \
     --workflow-type name=spam,version=1.1 \
     --task-list name=test-list \
     --child-policy ABANDON \
     | python3 -c 'import sys, json; print(json.load(sys.stdin)["runId"])' \
     > /tmp/runid

----

Pretend to be an activity worker

.. code-block:: shell

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf poll-for-activity-task \
     --domain test-domain --task-list name=eggs \
     | python3 -c 'import sys, json; print(json.load(sys.stdin)["taskToken"])' \
     > /tmp/tasktoken
   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf respond-activity-task-completed \
     --task-token $(cat /tmp/tasktoken)

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf poll-for-activity-task \
     --domain test-domain --task-list name=eggs \
     | python3 -c 'import sys, json; print(json.load(sys.stdin)["taskToken"])' \
     > /tmp/tasktoken
   aws --endpoint-url $AWS_SWF_ENDPOINT_URL swf respond-activity-task-completed \
     --task-token $(cat /tmp/tasktoken)

----

Check execution status

.. code-block:: shell

   aws --endpoint-url $AWS_SWF_ENDPOINT_URL describe-workflow-execution \
     --domain test-domain --execution workflowId=test-wf,runId=$(cat /tmp/runid)
