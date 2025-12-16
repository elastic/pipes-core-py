Elastic Pipes Core Documentation
================================

Elastic Pipes Core is a lightweight Python composition system that executes components
(called "pipes") in sequence, passing state through a shared dictionary.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting-started
   user-guide
   api-reference

Getting Started
---------------

Installation
~~~~~~~~~~~~

Install via pip:

.. code-block:: bash

   pip install elastic-pipes

Quick Example
~~~~~~~~~~~~~

Create a simple pipe:

.. code-block:: python

   from elastic.pipes.core import Pipe
   from typing_extensions import Annotated

   @Pipe("example.hello")
   def hello(
       name: Annotated[str, Pipe.Config("name")] = "World",
   ):
       return {"greeting": f"Hello, {name}!"}

Define a configuration file ``config.yaml``:

.. code-block:: yaml

   pipes:
     - example.hello:
         name: ReadTheDocs

Run the pipeline:

.. code-block:: bash

   elastic-pipes config.yaml

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
