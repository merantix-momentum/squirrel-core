Document
=================



This package comes with configuration and initial documentation for the documentation tool `Sphinx <https://www.sphinx-doc.org/en/master/>`_.

To build the documentation you can either ``cd`` into the package and use the ``devtools`` own ``mx build_docs`` command or rely on "native" Sphinx. To do so

.. code-block:: console

   cd docs/
   make html

or

.. code-block:: console

   python setup.py build_sphinx

in the main directory.

Sphinx comes with a number of available output formats that can be supplied instead of "html", for instance ``make latexpdf`` runs the pdfTeX toolchain and outputs documentation as PDF.

Configuration
-------------------

You can configure the documentation to your wish. This may include using a different `theme <https://sphinx-themes.org/>`_ (although using the default mx theme is preferred), setting a custom logo (set option ``logo`` in ``html_theme_options`` in ``docs/conf.py``) or extending HTML templates.

Multi-versioned Documentation
---------------------------------

You can build a multi-versioned documentation of this package with the help of `sphinx-versions <https://sphinx-versions.readthedocs.io//>`_. To do so, call ``sphinx-versioning build docs docs/_build/html`` which will build versioned documentation into ``docs/_build/html/``.


Hosting the documentation on app engine
------------------------------------------

The ``infrastructure/terraform`` folder contains infrastructure as code for setting up an app engine app in google cloud.

To add these resources to your google cloud project, remove the ``.disable`` suffix from  ``documentation.tf.disable`` and ``iap.tf.disable`` and perform a ``terraform apply``. This will

* create a Google App Engine application
* map the custom domain `docs.squirrel.merantixlabs.cloud <https://docs.squirrel.merantixlabs.cloud>`_ to the app engine project (and set DNS entries accordingly)
* enable IAP and the IAP brand to secure the documentation
* set up permissions & roles such that the cloudbuild service account can deploy the documentation

.. warning::

    In order to be able to call ``terraform apply`` on ``documentation.tf``, you have to be an owner of the group labs-technical-support@merantix.com!

.. warning::

    IAP is currently not enabled automatically for the app engine application. To do so, go `here <https://console.cloud.google.com/security/iap?project=mx-labs-devops>`_ and enable IAP on the App Engine app.

Deploying the documention
-----------------------------

To deploy the documentation with the newly created app engine project

.. code-block:: console

   cd docs/
   gcloud app deploy


You should then be able to see the documentation under https://docs.squirrel.merantixlabs.cloud. It should be secured with the IAP brand that was created earlier.

Continuous Deployment
------------------------------------------

To enable continuous deployment of the documentation, uncomment the blocks `build_docs` and `deploy_docs` in ``cloudbuild_master.yaml`` and ``cloudbuild_tag.yaml``.

.. warning::

    Make sure that the Github user `merantix-minion` has READ access to your repository.


