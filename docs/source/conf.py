# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
sys.path.insert(0, os.path.abspath('../../mlflow_provider/'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'MLflow Provider'
author = 'Astronomer Inc.'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "autoapi.extension",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

templates_path = ['_templates']
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_theme_options = {
    "description": "Airflow Provider containing Hooks and Operators for MLflow",
    "github_user": "astronomer",
    "github_repo": "airflow-provider-mlflow",
}

# -- AutoAPI ---------------------------------------------------------------
autoapi_dirs = ["../../mlflow_provider"]

# autoapi_template_dir = "_templates/autoapi"
autoapi_generate_api_docs = True

# The default options for autodoc directives. They are applied to all autodoc directives automatically.
autodoc_default_options = {"show-inheritance": True, "members": True}

autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autodoc_typehints_format = "short"

# Keep the AutoAPI generated files on the filesystem after the run.
# Useful for debugging.
autoapi_keep_files = True

# Relative path to output the AutoAPI files into. This can also be used to place the generated documentation
# anywhere in your documentation hierarchy.
autoapi_root = "_api"

# Whether to insert the generated documentation into the TOC tree. If this is False, the default AutoAPI
# index page is not generated and you will need to include the generated documentation in a
# TOC tree entry yourself.
autoapi_add_toctree_entry = True

# By default autoapi will include private members -- we don't want that!
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
]

# Ignore example DAGs from the API docs
autoapi_ignore = [
    "*example_dags*",
]

suppress_warnings = [
    "autoapi.python_import_resolution",
    "ref.doc",
]

autoapi_python_use_implicit_namespaces = True
