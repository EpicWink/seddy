Command-line  application
=========================

*seddy* provides a command-line interface for the as-built production service.
The interface documentation can be accessed with:

.. code-block:: shell

   seddy -h

Docker
------

Instead of installing `seddy` locally, you can use our pre-built Docker image
.. code-block:: shell

   docker run -v /path/to/workflow/file/parent:/seddy-data seddy -h
