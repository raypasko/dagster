User-defined types
------------------

We've seen how we can type the inputs and outputs of solids using Python 3's typing system, and
how to use Dagster's built-in config types, such as :py:class:`dagster.String`, to define config
schemas for our solids.

But what about when you want to define your own types?

Let's look back at our simple ``read_csv`` solid.

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/inputs_typed.py
   :lines: 6-14

The ``lines`` object returned by Python's built-in ``csv.DictReader`` is a list of
``collections.OrderedDict``, each of which represents one row of the dataset:

.. code-block:: python

    [
        OrderedDict([
            ('name', '100% Bran'), ('mfr', 'N'), ('type', 'C'), ('calories', '70'), ('protein', '4'),
            ('fat', '1'), ('sodium', '130'), ('carbo', '5'), ('sugars', '6'), ('potass', '280'),
            ('vitamins', '25'), ('shelf', '3'), ('weight', '1'), ('cups', '0.33'),
            ('rating', '68.402973')
        ]),
        OrderedDict([
            ('name', '100% Natural Bran'), ('mfr', 'Q'), ('type', 'C'), ('calories', '120'),
            ('protein', '3'), ('fat', '5'), ('sodium', '15'), ('fiber', '2'), ('carbo', '8'),
            ('sugars', '8'), ('potass', '135'), ('vitamins', '0'), ('shelf', '3'), ('weight', '1'),
            ('cups', '1'), ('rating', '33.983679')
        ]),
        ...
    ]

This is a simple representation of a "data frame", or a table of data. We'd like to be able to
use Dagster's type system to type the output of ``read_csv``, so that we can do type checking when
we construct the pipeline, ensuring that any solid consuming the output of ``read_csv`` expects to
receive a data frame.

To do this, we'll use the :py:func:`@dagster_type <dagster.dagster_type>` decorator:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 3-14
   :caption: custom_types.py

Now we can annotate the rest of our pipeline with our new type:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 17-45
   :emphasize-lines: 9, 13
   :caption: custom_types.py


The type metadata now appears in dagit and the system will ensure the input and output to this
solid are indeed instances of SimpleDataFrame. As usual, run:

.. code-block:: console

   $ dagit -f custom_types.py -n custom_type_pipeline

.. thumbnail:: config_figure_two.png

You can see that the output of ``read_csv`` (which by default has the name ``result``) is marked
to be of type ``SimpleDataFrame``.


Custom type checks and metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Dagster framework will fail type checks when solids are wired up incorrectly -- e.g., if
an input of type ``int`` depends on an output of type ``SimpleDataFrame`` -- or when a value isn't
an instance of the type we're expecting, e.g., if ``read_csv`` were to return a ``str`` rather than
a ``SimpleDataFrame``.

Sometimes we know more about the types of our values, and we'd like to do deeper type checks. For
example, in the case of the ``SimpleDataFrame``, we expect to see a list of OrderedDicts, and for
each of these OrderedDicts to have the same fields, in the same order.

The :py:func:`@dagster_type <dagster.dagster_type>` decorator lets us specify custom type checks
like this.

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types_2.py
   :lines: 2-39
   :emphasize-lines: 6, 35
   :caption: custom_types_2.py

Now, if our solid logic fails to return the right type, we'll see a type check failure. Let's
replace our ``read_csv`` solid with the following bad logic:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types_2.py
   :lines: 44-52
   :caption: custom_types_2.py

When we run the pipeline with this solid, we'll see an error like:

.. code-block:: console

    2019-10-12 13:19:19 - dagster - ERROR - custom_type_pipeline - 266c6a93-75e2-46dc-8bd7-d684ce91d0d1 - STEP_FAILURE - Execution of step "bad_read_csv.compute" failed.
                cls_name = "Failure"
                solid = "bad_read_csv"
        solid_definition = "bad_read_csv"
                step_key = "bad_read_csv.compute"
    user_failure_data = {"description": "LessSimpleDataFrame should be a list of OrderedDicts, got <class 'str'> for row 1", "label": "intentional-failure", "metadata_entries": []}

Custom type can also yield metadata about the type check. For example, in the case of our data
frame, we might want to record the number of rows in the dataset.


Input Hydration Config
^^^^^^^^^^^^^^^^^^^^^^

This solid as defined is only expressed in terms of an in-memory object; it says nothing about
how this data should be sourced from or materialized to disk. This is where the notion of
input hydration and output materialization configs come into play. Once the user provides those
she is able to use the configuration language in order to parameterize the computation. Note:
one can always just write code in an upstream solid *instead* of using the configuration
system to do so.

Let us now add the input hydration config:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 21-24
   :linenos:

Then insert this into the original declaration:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 34-35,44-47
   :emphasize-lines: 2
   :linenos:

Now if you run a pipeline with this solid from dagit you will be able to provide sources for
these inputs via config:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_type_input.yaml
   :linenos:


Output Materialization Config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We will add output materialization config now. They are similar to input hydration config, except
that they are responsible for taking the in-memory object flowed through your computation and
materializing it to some persistent store. Outputs are purely *optional* for any computation,
whereas inputs *must* be provided for a computation to proceed. You will likely want outputs as for
a pipeline to be useful it should produce some materialization that outlives the computation.

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 27-31
   :linenos:

This has a similar aesthetic to an input hydration config but performs a different function. Notice that
it takes a third argument, ``sauce`` (it can be named anything), that is the value that was
outputted from the solid in question. It then takes the configuration data as "instructions" as to
how to materialize the value.

One connects the output materialization config to the type as follows:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 34-36,44-47
   :emphasize-lines: 3
   :linenos:

Now we can provide a list of materializations to a given execution.

You'll note you can provide an arbitrary number of materializations. You can materialize any
given output any number of times in any number of formats.

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_type_output.yaml
   :linenos:


Type Metadata
^^^^^^^^^^^^^
User-defined types can also provide a metadata function that returns a :py:class:`TypeCheck <dagster.TypeCheck>`
object containing metadata entries about an instance of the type when it successfully passes the instanceof check.

We will use this to emit some summary statistics about our DataFrame type:

.. literalinclude:: ../../../../examples/dagster_examples/intro_tutorial/custom_types.py
   :lines: 34-47
   :emphasize-lines: 4-10
   :linenos:


Data quality checks
-------------------


Materializations and intermediates
----------------------------------
