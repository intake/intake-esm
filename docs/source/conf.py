# -*- coding: utf-8 -*-

# import inspect
import datetime
import os
import sys

import yaml

import intake_esm

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
# sys.path.insert(0, os.path.abspath('.'))

cwd = os.getcwd()
parent = os.path.dirname(cwd)
sys.path.insert(0, parent)


# -- General configuration -----------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    # 'sphinx.ext.linkcode',
    'sphinx.ext.intersphinx',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive',
    'sphinx.ext.napoleon',
    'myst_nb',
    'sphinxext.opengraph',
    'sphinx_copybutton',
    'sphinx_comments',
]


# MyST config
myst_enable_extensions = ['amsmath', 'colon_fence', 'deflist', 'html_image']
myst_url_schemes = ('http', 'https', 'mailto')

# sphinx-copybutton configurations
copybutton_prompt_text = r'>>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: '
copybutton_prompt_is_regexp = True

comments_config = {
    'utterances': {'repo': 'intake/intake-esm', 'optional': 'config', 'label': '💬 comment'},
    'hypothesis': False,
}


execution_timeout = 600

extlinks = {
    'issue': ('https://github.com/intake/intake-esm/issues/%s', 'GH#'),
    'pr': ('https://github.com/intake/intake-esm/pull/%s', 'GH#'),
}
# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# Autosummary pages will be generated by sphinx-autogen instead of sphinx-build
autosummary_generate = []
autodoc_typehints = 'none'
autodoc_member_order = 'groupwise'

# Napoleon configurations

napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_use_param = False
napoleon_use_rtype = False
napoleon_preprocess_types = True


# The suffix of source filenames.
# source_suffix = '.rst'

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
current_year = datetime.datetime.now().year
project = u'Intake-ESM'
copyright = f'2018-{current_year}, Intake-ESM development team'
author = u'Intake-ESM developers'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = intake_esm.__version__.split('+')[0]
# The full version, including alpha/beta/rc tags.
release = intake_esm.__version__


# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build', '**.ipynb_checkpoints', 'Thumbs.db', '.DS_Store']


# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'


# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'furo'
html_title = ''


# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = '../_static/images/NSF_4-Color_bitmap_Logo.png'
html_context = {
    'github_user': 'intake',
    'github_repo': 'intake-esm',
    'github_version': 'main',
    'doc_path': 'docs',
}
html_theme_options = {}


# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['../_static']

# Sometimes the savefig directory doesn't exist and needs to be created
# https://github.com/ipython/ipython/issues/8733
# becomes obsolete when we can pin ipython>=5.2; see ci/requirements/doc.yml
ipython_savefig_dir = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '_build', 'html', '_static'
)

savefig_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'source', '_static')

os.makedirs(ipython_savefig_dir, exist_ok=True)
os.makedirs(savefig_dir, exist_ok=True)

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = '%b %d, %Y'


# Output file base name for HTML help builder.
htmlhelp_basename = 'intake_esmdoc'


# -- Options for LaTeX output --------------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    # 'papersize': 'letterpaper',
    # The font size ('10pt', '11pt' or '12pt').
    # 'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    # 'preamble': '',
}


latex_documents = [('index', 'intake-esm.tex', u'intake-esm Documentation', author, 'manual')]

man_pages = [('index', 'intake-esm', u'intake-esm Documentation', [author], 1)]

texinfo_documents = [
    (
        'index',
        'intake-esm',
        u'intake-esm Documentation',
        author,
        'intake-esm',
        'One line description of project.',
        'Miscellaneous',
    )
]

ipython_warning_is_error = False
ipython_execlines = [
    'import intake',
    'import intake_esm',
    'import xarray',
    'import pandas as pd',
    'pd.options.display.encoding="utf8"',
]


intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'xarray': ('http://xarray.pydata.org/en/stable/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'intake': ('https://intake.readthedocs.io/en/latest/', None),
}


# based on numpy doc/source/conf.py


# def linkcode_resolve(domain, info):
#     """
#     Determine the URL corresponding to Python object
#     """
#     if domain != 'py':
#         return None

#     modname = info['module']
#     fullname = info['fullname']

#     submod = sys.modules.get(modname)
#     if submod is None:
#         return None

#     obj = submod
#     for part in fullname.split('.'):
#         try:
#             obj = getattr(obj, part)
#         except AttributeError:
#             return None

#     try:
#         fn = inspect.getsourcefile(inspect.unwrap(obj))
#     except TypeError:
#         fn = None
#     if not fn:
#         return None

#     try:
#         source, lineno = inspect.getsourcelines(obj)
#     except OSError:
#         lineno = None

#     if lineno:
#         linespec = f'#L{lineno}-L{lineno + len(source) - 1}'
#     else:
#         linespec = ''

#     fn = os.path.relpath(fn, start=os.path.dirname(intake_esm.__file__))

#     if '+' in intake_esm.__version__:
#         return f'https://github.com/intake/intake-esm/blob/master/intake_esm/{fn}{linespec}'
#     else:
#         return (
#             f'https://github.com/intake/intake-esm/blob/'
#             f'v{intake_esm.__version__}/intake_esm/{fn}{linespec}'
#         )


# https://www.ericholscher.com/blog/2016/jul/25/integrating-jinja-rst-sphinx/


def rstjinja(app, docname, source):
    """
    Render our pages as a jinja template for fancy templating goodness.
    """
    # Make sure we're outputting HTML
    if app.builder.format != 'html':
        return
    src = source[0]
    rendered = app.builder.templates.render_string(src, app.config.html_context)
    source[0] = rendered


def html_page_context(app, pagename, templatename, context, doctree):
    # Disable edit button for docstring generated pages
    if 'generated' in pagename:
        context['theme_use_edit_page_button'] = False


def setup(app):
    app.connect('source-read', rstjinja)
    app.connect('html-page-context', html_page_context)


with open('catalogs.yaml') as f:
    catalogs = yaml.safe_load(f)


html_context = {'catalogs': catalogs['catalogs']}
