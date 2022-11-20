File Liked Object IO
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:

`File Object <https://docs.python.org/3/glossary.html#term-file-object>`_ is an object exposing a file-oriented API (with methods such as read() or write()) to an underlying resource. Depending on the way it was created, a file object can mediate access to a real on-disk file or to another type of storage or communication device (for example standard input/output, in-memory buffers, sockets, pipes, etc.). File objects are also called file-like objects or streams.


Single S3 Object Text / Bytes IO
------------------------------------------------------------------------------
- :meth:`read text <~s3pathlib.core.rw.ReadAndWriteAPIMixin.read_text>`
- :meth:`read bytes  <~s3pathlib.core.rw.ReadAndWriteAPIMixin.read_bytes>`
- :meth:`write text  <~s3pathlib.core.rw.ReadAndWriteAPIMixin.write_text>`
- :meth:`write bytes  <~s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes>`

.. code-block:: python

    p = S3Path("bucket", "file.dat")

    # write text to s3 object
    p.write_text("Hello World")

    # read text from s3 object
    text = p.read_text() # by default it use utf-8 encoding

    # write binary to s3 object
    p.write_bytes("this is binary".encode("utf-8"))

    # read binary from s3 object
    b = p.read_bytes()


File IO API open / close, read / write
------------------------------------------------------------------------------
.. note::

    Special Thanks to `smart_open <https://github.com/RaRe-Technologies/smart_open>`_. :meth:`S3Path.open <s3pathlib.core.opener.OpenerAPIMixin.open>` just provides a simple wrapper around ``smart_open``.

.. code-block:: python

    p = S3Path("bucket", "file.dat")

    # p.open is a context manager, can auto close after writes
    with p.open("w") as f:
        f.write("Hello World")

    # stream very big S3 file line by line for memory saving
    with p.open("r") as f:
        text = f.read()


Integrate With Other IO tools
------------------------------------------------------------------------------
**JSON**

.. code-block:: python

    import json

    p = S3Path("bucket", "data.json")

    # dump json to s3 object
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    with p.open("w") as f:
        json.dump(data, f)

    # read json from s3 object
    with p.open("r") as f:
        data = json.load(f)

**Pandas**

``s3pathlib`` + ``pandas`` allows you to read / write many popular data format from / to s3 object. See https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html for more information.

.. code-block:: python

    import pandas as pd # pip instal pandas

    df = pd.DataFrame(
        [
            (1, "Alice"),
            (2, "Bob"),
        ],
        columns=["id", "name"]
    )

    # dump csv to s3 object
    with p.open("w") as f:
        df.to_csv(f, index=False)

    # read dataframe from s3 object
    with p.open("r") as f:
        df = pd.read_csv(f)

**YAML**

.. code-block:: python

    import yaml # pip install PyYaml

    data = {
        "secret": {
            "username": "myusername",
            "password": "mypassword",
        }
    }


    # dump yaml to s3 object
    with p.open("w") as f:
        yaml.dump(data, f)

    # read yaml from s3 object
    with p.open("r") as f:
        data = yaml.load(f)
