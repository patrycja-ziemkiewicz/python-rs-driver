from __future__ import annotations

import sys
import tomllib
from pathlib import Path

from sphinx_scylladb_theme.utils import (  # pyright: ignore[reportMissingTypeStubs]
    multiversion_regex_builder,  # pyright: ignore[reportUnknownVariableType]
)

DOCS_SOURCE = Path(__file__).resolve().parent
DOCS_DIR = DOCS_SOURCE.parent
REPO_ROOT = DOCS_DIR.parent
PYTHON_SOURCE = REPO_ROOT / "python"

# Local extensions.
sys.path.insert(0, str(DOCS_SOURCE / "_ext"))


# -- Global variables --------------------------------------------------

# Publish only the default branch for now, under the stable URL.
TAGS: list[str] = []

BRANCHES = ["main"]

LATEST_VERSION = "main"

UNSTABLE_VERSIONS = []

DEPRECATED_VERSIONS = []


# -- General configuration ---------------------------------------------

extensions = [
    "autoapi.extension",
    "autoapi_fixes",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "sphinx_sitemap",
    "sphinx_multiversion",
    "myst_parser",
    "sphinx_scylladb_theme",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

master_doc = "index"

project = "ScyllaDB Python RS Driver"

copyright = "2026, ScyllaDB"

author = "ScyllaDB"

with (REPO_ROOT / "Cargo.toml").open("rb") as cargo_toml:
    release = tomllib.load(cargo_toml)["package"]["version"]

version = release

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "**/_partials",
    # The package page would only repeat the module table of api/index.md.
    "api/reference/scylla/index.rst",
]

# Hide parent class names in sidebar navigation.
toc_object_entries_show_parents = "hide"


# -- Options for API reference -----------------------------------------

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


# -- Options for myst parser -------------------------------------------

myst_enable_extensions = ["colon_fence"]

myst_heading_anchors = 3


# -- Options for not found extension -----------------------------------

# Use the theme's 404 page with assets served from the custom domain root.
notfound_template = "404.html"

notfound_urls_prefix = ""


# -- Options for sitemap extension -------------------------------------

sitemap_url_scheme = "/stable/{link}"


# -- Options for multiversion extension --------------------------------

smv_tag_whitelist = multiversion_regex_builder(TAGS)

smv_branch_whitelist = multiversion_regex_builder(BRANCHES)

smv_latest_version = LATEST_VERSION

smv_rename_latest_version = "stable"

smv_remote_whitelist = r"^origin$"

smv_released_pattern = r"^tags/.*$"

smv_outputdir_format = "{ref.name}"


# -- Options for HTML output -------------------------------------------

html_theme = "sphinx_scylladb_theme"

html_theme_options = {  # type: ignore
    "conf_py_path": "docs/source/",
    "github_repository": "scylladb/python-rs-driver",
    "github_issues_repository": "scylladb/python-rs-driver",
    "default_branch": "main",
    "site_description": "Documentation for the asynchronous ScyllaDB Python RS Driver.",
    "hide_edit_this_page_button": "false",
    "hide_feedback_buttons": "false",
    "hide_version_dropdown": [],
    "versions_unstable": UNSTABLE_VERSIONS,
    "versions_deprecated": DEPRECATED_VERSIONS,
    # API reference pages are named after modules, like policies/load_balancing.
    "skip_warnings": ["document_has_underscores"],
}

html_title = "ScyllaDB Python RS Driver Documentation"

html_short_title = "Python RS Driver Docs"

html_sidebars = {"**": ["side-nav.html"]}

# Root URL for the multiversion GitHub Pages deployment.
html_baseurl = "https://python-rs-driver.docs.scylladb.com"

html_context = {"html_baseurl": html_baseurl}
