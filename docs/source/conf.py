# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Yet Another Simple Scraper"
copyright = "2023, Ivan Danchuk"
author = "Ivan Danchuk"
release = "0.0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.napoleon",
    "sphinx.ext.doctest",
    "sphinx.ext.duration",
    "sphinx.ext.inheritance_diagram",
    "sphinx_click",
    "sphinx_autodoc_typehints",
    "sphinx_tabs.tabs",
    "sphinx_last_updated_by_git",
    # "sphinx_sitemap",
    "sphinx_paramlinks",
    "sphinx_lfs_content",
    "seed_intersphinx_mapping",
    "sphinx_copybutton",
    # "sphinx_togglebutton",
    "sphinx_design",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "notfound.extension",
    "sphinxawesome_theme",
]
myst_enable_extensions = ["colon_fence"]

extensions += ["sphinxcontrib.autodoc_inherit_overload"]

templates_path = ["_templates"]
language = "ru"
# locale_dirs = ['locale/', 'docs/']
# gettext_compact = False
primary_domain = "py"
default_domain = "py"
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
pygments_style = "nord-darker"


# -- Options for autodoc -------------------------------------------------
autodoc_default_flags = ["members", "inherited-members"]

# TODO: тайпхинты возможно нужно будет отрубить из-за использования дополнительного расширения
autodoc_typehints = "both"
autodoc_typehints_format = "short"

# -- Options for autosectionlabel ----------------------------------------
autosectionlabel_prefix_document = True

# -- Options for sphinx_autodoc_typehints --------------------------------
always_document_param_types = True
typehints_defaults = "comma"
typehints_use_signature = True
typehints_use_signature_return = True

# -- Options for sphinx-tabs ---------------------------------------------
sphinx_tabs_valid_builders = ["linkcheck"]

# -- Options for sphinx_last_updated_by_git ------------------------------
git_untracked_check_dependencies = False
git_untracked_show_sourcelink = True

# -- Options for sphinx_paramlinks ---------------------------------------
paramlinks_hyperlink_param = "name"

# -- Options for todo ----------------------------------------------------
todo_include_todos = True


repository_root = "../.."
pkg_requirements_source = "pyproject.toml"

# -- Options for HTML output ---------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinxawesome_theme"
html_static_path = ["_static"]
html_sidebars = {"**": ["sidebar_main_nav_links.html", "sidebar_toc.html"]}
