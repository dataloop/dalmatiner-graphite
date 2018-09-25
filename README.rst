.. _readme:

`dalmatiner-graphite_
----------------------------

.. code-block:: none

    pip install --upgrade ddbpy

A simple daemon that listens on UDP port 2003 (localhost) for incoming graphite data and forwards to a Dalmatiner DB backend on port 5555 (localhost).

handle data in this formats:

.. code-block:: none

	   local.random.diceroll 9.2 1537515876
	   
will be stored in ddb as:

.. code-block:: none

	   local.random.diceroll 9.2 1537515876
	   
and

.. code-block:: none

	   local.random.diceroll.1;2;3;4 9.2;1.1;2.5;4.3 1537515876
	  
will be stored in ddb as

.. code-block:: none

	   local.random.diceroll.1 9.2 1537515876
	   local.random.diceroll.2 1.1 1537515876
	   local.random.diceroll.3 2.5 1537515876
	   local.random.diceroll.4 4.3 1537515876
