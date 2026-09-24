from __future__ import annotations

import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DOCS_SOURCE = Path(__file__).resolve().parent
DOCS_DIR = DOCS_SOURCE.parent
REPO_ROOT = DOCS_DIR.parent

PYTHON_SOURCE = REPO_ROOT / "python"

# Local extensions.
sys.path.insert(0, str(DOCS_SOURCE / "_ext"))


# ---------------------------------------------------------------------------
# Project Metadata
# ---------------------------------------------------------------------------

project = "ScyllaDB Python RS Driver"
copyright = "2026, ScyllaDB"
author = "ScyllaDB"
version = "0.1.0"
release = "0.1.0"
html_title = "ScyllaDB Python RS Driver Documentation"
html_short_title = "Python RS Driver Docs"

# Master document setting
master_doc = "contents"


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------

extensions = [
    "autoapi.extension",
    "autoapi_fixes",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_scylladb_theme",
]


# ---------------------------------------------------------------------------
# ScyllaDB Official Theme Configuration
# ---------------------------------------------------------------------------

html_theme = "sphinx_scylladb_theme"

# The theme's templates use html_baseurl, which Sphinx 9 no longer passes to them.
html_baseurl = ""
html_context = {"html_baseurl": html_baseurl}

html_theme_options = {  # type: ignore
    "conf_py_path": "docs/source/",
    "github_repository": "scylladb-zpp-2025-python-rs-driver/python-rs-driver",
    "github_issues_repository": "scylladb-zpp-2025-python-rs-driver/python-rs-driver",
    "hide_edit_this_page_button": False,
    "hide_feedback_buttons": False,
    "hide_version_dropdown": [],
}

# Explicitly wire up the custom sidebar layout from the theme
html_sidebars = {"**": ["side-nav.html"]}

# Hide parent class names in sidebar for cleaner navigation trees
toc_object_entries_show_parents = "hide"


# ---------------------------------------------------------------------------
# Source formats
# ---------------------------------------------------------------------------

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}


# ---------------------------------------------------------------------------
# API reference
# ---------------------------------------------------------------------------

# Stub docstrings list their properties under "Attributes:", which would
# otherwise describe each one a second time.
napoleon_use_ivar = True

# Parsed statically, so the .pyi stubs of scylla._rust supply the classes
# and the Rust extension does not have to be built.
autoapi_dirs = [str(PYTHON_SOURCE / "scylla")]
autoapi_file_patterns = ["*.pyi", "*.py"]
autoapi_root = "api/reference"
autoapi_add_toctree_entry = False
# The theme's llms.txt build runs in parallel on the same sources, and deleting
# the generated files when either build ends breaks the other one.
autoapi_keep_files = True
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]

# Link standard library types in signatures to the Python docs.
intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# Show `Batch` rather than `scylla.statement.Batch` in signatures.
python_use_unqualified_type_names = True

# autoapi cannot follow the import cycles between the stubs, like routing <-> cluster.
suppress_warnings = ["autoapi.python_import_resolution"]

# The package page would only repeat the module table of api/index.md.
exclude_patterns = ["api/reference/scylla/index.rst"]
