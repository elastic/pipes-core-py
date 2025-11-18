Getting Started
===============

Overview
--------

Elastic Pipes Core defines a composition system where components (pipes) execute
in sequence, passing state through a shared dictionary. Each pipe can read
from and write to this state, enabling data flow between components.

Key Concepts
------------

Pipes
~~~~~

Pipes are functions decorated with ``@Pipe`` that process state data.
They declare their configuration and state dependencies using type annotations.

State
~~~~~

A dictionary passed through the pipe sequence. Each pipe can read from
and write to the state. State is modified in place and forwarded to
subsequent pipes.

Configuration
~~~~~~~~~~~~~

Each pipe has its own configuration section in the YAML file. Configuration
values are bound to function parameters using ``Pipe.Config``.

Installation
------------

Install from PyPI:

.. code-block:: bash

   pip install elastic-pipes

Or install from source:

.. code-block:: bash

   git clone https://github.com/elastic/pipes-py.git
   cd pipes-py
   pip install .

Basic Usage
-----------

1. Create a Pipe
~~~~~~~~~~~~~~~~

Create a Python file with your pipe definition:

.. code-block:: python

   # my_pipes.py
   from elastic.pipes.core import Pipe
   from typing_extensions import Annotated
   import logging

   @Pipe("example.process")
   def process(
       threshold: Annotated[int, Pipe.Config("threshold")] = 10,
       data: Annotated[list, Pipe.State("input")],
       results: Annotated[list, Pipe.State("output", mutable=True)],
       log: logging.Logger,
   ):
       """Filter data above threshold."""
       log.info(f"Processing {len(data)} items")
       results.extend([x for x in data if x > threshold])

2. Create Configuration
~~~~~~~~~~~~~~~~~~~~~~~

Create a YAML configuration file:

.. code-block:: yaml

   # pipeline.yaml
   pipes:
     - elastic.pipes.core.import:
         file: data.json
         node: input
     
     - example.process:
         threshold: 50
     
     - elastic.pipes.core.export:
         file: results.json
         node: output

3. Run the Pipeline
~~~~~~~~~~~~~~~~~~~

Execute the pipeline:

.. code-block:: bash

   elastic-pipes pipeline.yaml

UNIX Pipe Mode
--------------

Individual pipes can run as standalone commands in UNIX pipe mode:

.. code-block:: bash

   # Generate state and pipe through commands
   echo '{"pipes": [{"example.process": {"threshold": 20}}], "input": [10, 25, 50]}' | \\
     python my_pipes.py --pipe-mode | \\
     elastic-pipes.core.export --pipe-mode --file output.json

Get help for a pipe:

.. code-block:: bash

   python my_pipes.py --describe

Next Steps
----------

- Read the :doc:`user-guide` for detailed usage patterns
- See the :doc:`api-reference` for complete API documentation
