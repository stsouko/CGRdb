# -*- coding: utf-8 -*-
import CGRdb
from LazyPony import LazyEntityMeta
from os.path import abspath
from pony.orm import Database
from sys import path
parent = abspath('..')
if parent not in path:
    path.insert(0, parent)
LazyEntityMeta.attach(Database(), database='CGRdb')

author = 'Dr. Ramil Nugmanov'
copyright = '2017-2020, Dr. Ramil Nugmanov <nougmanoff@protonmail.com>'
version = '4.0'
project = 'CGRdb'

needs_sphinx = '1.8'
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.autosummary', 'm2r', 'nbsphinx']

nbsphinx_kernel_name = 'python3'

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '**.ipynb_checkpoints']
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

language = None
pygments_style = 'flasky'
todo_include_todos = False
autoclass_content = 'both'

html_theme_options = {'github_user': 'stsouko', 'github_repo': 'CGRdb', 'show_related': True}
html_show_copyright = True
html_show_sourcelink = False
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',  # needs 'show_related': True theme option to display
        'searchbox.html',
    ]
}
