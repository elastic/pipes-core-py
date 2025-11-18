User Guide
==========

This guide covers common usage patterns and advanced features of Elastic Pipes Core.

Writing Pipes
-------------

Basic Pipe Structure
~~~~~~~~~~~~~~~~~~~~

A pipe is a function decorated with ``@Pipe``:

.. code-block:: python

   from elastic.pipes.core import Pipe
   from typing_extensions import Annotated
   import logging

   @Pipe("my.example")
   def example(
       config_param: Annotated[str, Pipe.Config("param")],
       state_data: Annotated[dict, Pipe.State("data")],
       log: logging.Logger,
   ):
       """Process data from state."""
       log.info(f"Processing with {config_param}")
       return {"result": "processed"}

Parameter Binding
~~~~~~~~~~~~~~~~~

Pipe.Config
^^^^^^^^^^^

Bind parameters to configuration nodes:

.. code-block:: python

   threshold: Annotated[int, Pipe.Config("threshold")]

Configuration in YAML:

.. code-block:: yaml

   pipes:
     - my.pipe:
         threshold: 100

Pipe.State
^^^^^^^^^^

Bind parameters to state nodes:

.. code-block:: python

   # Read-only access
   data: Annotated[list, Pipe.State("documents")]
   
   # Mutable access
   results: Annotated[list, Pipe.State("output", mutable=True)]
   
   # Whole state
   state: Annotated[dict, Pipe.State(None)]

Indirect References
^^^^^^^^^^^^^^^^^^^

Configuration can specify which state node to use:

.. code-block:: python

   data: Annotated[list, Pipe.Config("data")]

Configuration:

.. code-block:: yaml

   pipes:
     - my.pipe:
         data@: input.documents  # @ suffix means state reference

Context Managers
~~~~~~~~~~~~~~~~

Group related parameters in a context:

.. code-block:: python

   class ElasticsearchContext(Pipe.Context):
       url: Annotated[str, Pipe.Config("elasticsearch.url")]
       index: Annotated[str, Pipe.Config("index")]
       
   @Pipe("my.es")
   def process(ctx: ElasticsearchContext, log: logging.Logger):
       log.info(f"Connecting to {ctx.url}")

Help and Documentation
~~~~~~~~~~~~~~~~~~~~~~

Add help text and notes to parameters:

.. code-block:: python

   threshold: Annotated[
       int,
       Pipe.Config("threshold"),
       Pipe.Help("minimum value to process"),
       Pipe.Notes("default: 10"),
   ] = 10

Pipeline Configuration
----------------------

Basic Structure
~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipes:
     - pipe.name:
         config: value
     - another.pipe:
         setting: value

Logging Configuration
~~~~~~~~~~~~~~~~~~~~~

Set log level per pipe:

.. code-block:: yaml

   pipes:
     - my.pipe:
         logging:
           level: debug

Runtime Arguments
~~~~~~~~~~~~~~~~~

Pass arguments from command line:

.. code-block:: bash

   elastic-pipes config.yaml --argument threshold=50 --argument format=json

Access in pipe:

.. code-block:: python

   threshold: Annotated[
       int,
       Pipe.Config("threshold@"),
   ]

Configuration with ``threshold@: runtime.arguments.threshold``.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

Similarly for environment variables:

.. code-block:: python

   api_key: Annotated[
       str,
       Pipe.Config("api-key@"),
   ]

Configuration with ``api-key@: runtime.environment.API_KEY``.

Built-in Pipes
--------------

elastic.pipes.core.import
~~~~~~~~~~~~~~~~~~~~~~~~~

Import data from files:

.. code-block:: yaml

   - elastic.pipes.core.import:
       file: input.json
       node: data
       format: json

Streaming support for NDJSON:

.. code-block:: yaml

   - elastic.pipes.core.import:
       file: large-dataset.ndjson
       streaming: true

elastic.pipes.core.export
~~~~~~~~~~~~~~~~~~~~~~~~~

Export data to files:

.. code-block:: yaml

   - elastic.pipes.core.export:
       file: output.yaml
       node: results

HCP Vault Integration
---------------------

Reading Secrets
~~~~~~~~~~~~~~~

.. code-block:: yaml

   - elastic.pipes.hcp.vault.read:
       url: https://vault.example.com
       token: hvs.xxx
       path: secret/data/myapp

Or use environment variables:

.. code-block:: bash

   export VAULT_ADDR=https://vault.example.com
   export VAULT_TOKEN=hvs.xxx

.. code-block:: yaml

   - elastic.pipes.hcp.vault.read:
       path: secret/data/myapp

Writing Secrets
~~~~~~~~~~~~~~~

.. code-block:: yaml

   - elastic.pipes.hcp.vault.write:
       path: secret/data/myapp

Testing Pipes
-------------

Use the test utilities:

.. code-block:: python

   from elastic.pipes.core import test
   import logging

   logger = logging.getLogger("test")
   
   with test.run(
       "my.pipe",
       {"threshold": 10},
       {"input": [5, 15, 25]},
       logger,
   ) as state:
       assert state["output"] == [15, 25]

Dry Run Mode
------------

Test pipeline without side effects:

.. code-block:: bash

   elastic-pipes config.yaml --dry-run

Pipes can support dry-run mode:

.. code-block:: python

   @Pipe("my.pipe")
   def process(data: ..., dry_run: bool = False):
       if not dry_run:
           # Perform actual operations
           write_to_database(data)
