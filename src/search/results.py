import os
from functools import lru_cache
from pathlib import Path

import pandas as pd
from gensim.summarization import summarizer
from gensim.summarization.textcleaner import get_sentences
from jinja2 import Template


# Convert the doi to a url
def doi_url(d):
    if not d:
        return '#'
    return f'http://{d}' if d.startswith('doi.org') else f'http://doi.org/{d}'

@lru_cache()
def get_root_dir() -> str:
    path = Path().cwd()
    while Path(path, "__init__.py").exists():
        path = path.parent
    return str(path)

# @lru_cache(maxsize=16)
def load_template(template):
    template_dir = os.path.join(get_root_dir(), 'templates')
    template_file = os.path.join(template_dir, f'{template}.template')
    with open(template_file, 'r') as f:
        return Template(f.read())


def render_html(template_name, **kwargs):
    template = load_template(template_name)
    return template.render(kwargs)


def shorten(text, length=200):
    if text:
        _len = min(len(text), length)
        shortened_text = text[:_len]
        ellipses = '...' if len(shortened_text) < len(text) else ''
        return f'{shortened_text}{ellipses}'
    return ''

def num_sentences(text):
    if not text:
        return 0
    return len(list(get_sentences(text)))


def summarize(text, word_count=120):
    if num_sentences(text) > 1:
        try:
            word_count_summary = summarizer.summarize(text, word_count=word_count)
        except ValueError:
            return text
        if word_count_summary:
            return word_count_summary
        else:
            ratio_summary = summarizer.summarize(text, ratio=0.2)
            if ratio_summary:
                return ratio_summary
    return text


class SearchResults:

    def __init__(self, data: pd.DataFrame, view='html'):
        self.results = data.dropna(subset=['title'])
        self.results['summary'] = self.results.abstract.apply(summarize)
        self.columns = [col for col in ['title', 'summary', 'publish_time'] if col in self.results]
        self.view = view

    def __len__(self):
        return len(self.results)

    def _view_html(self, search_results):
        _results = [{'title': rec['title'],
                     'abstract': shorten(rec['abstract'], 300),
                     'summary': shorten(summarize(rec['abstract']), 500),
                     'url': rec['url'],
                     'score': rec['Score'],
                     'publish_time': rec['publish_time']
                     }
                    for rec in search_results.to_dict('records')]
        return render_html('SearchResultsHTML', search_results=_results)

    def get_results_df(self):
        display_cols = [col for col in self.columns if not col == 'sha']
        return self.results[display_cols]

    def _repr_html_(self):
        if self.view == 'html':
            return self._view_html(self.results)
        elif any([self.view == v for v in ['df', 'dataframe', 'table']]):
            return self.get_results_df()._repr_html_()
        else:
            return self._view_html(self.results)