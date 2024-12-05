import time
import re
from functools import lru_cache
from pathlib import Path

import nltk
import pandas as pd
import boto3
import numpy as np
from rank_bm25 import BM25Okapi

from src.utils.consts import SIMPLE_STOPWORDS, AWS_PROFILE, AWS_REGION
from src.search.results import SearchResults
from src.processing.athena import AthenaWrapper


TOKEN_PATTERN = re.compile('^(20|19)\d{2}|(?=[A-Z])[\w\-\d]+$', re.IGNORECASE)
SARS_COV_2_DATE = '2022-01-01'


class BM25SearchEngine:

    def __init__(self, df: pd.DataFrame, search_column: str = 'abstract'):
        self.df = df
        self.search_column = search_column
        self.bm25_index = self.create_index_tokens(search_column)


    def _replace_punctuation(self, text: str):
        t = re.sub('\(|\)|:|,|;|\.|’|”|“|\?|%|>|<|≥|≤|~|`', '', text)
        t = re.sub('/', ' ', t)
        t = t.replace("'", '')
        return t

    def _clean(self, text: str):
        t = text.lower()
        t = self._replace_punctuation(t)
        return t

    def _tokenize(self, text):
        words = nltk.word_tokenize(text)
        return [word for word in words
                if len(word) > 1
                and not word in SIMPLE_STOPWORDS
                and TOKEN_PATTERN.match(word)]

    def _preprocess(self, text):
        t = self._clean(text)
        tokens = self._tokenize(t)
        return tokens

    @classmethod
    def get_bm25Okapi(cls, index_tokens: pd.Series) -> BM25Okapi:
        has_tokens = index_tokens.apply(len).sum() > 0
        if not has_tokens:
            index_tokens.loc[0] = ['no', 'tokens']
        return BM25Okapi(index_tokens.tolist())

    @lru_cache()
    def create_index_tokens(self, search_column: str) -> BM25Okapi:
        print('Creating the BM25 index from the abstracts of the papers')
        print('Use index="text" if you want to index the texts of the paper instead')
        tick = time.time()

        self.df['index_tokens'] = self.df[search_column].apply(self._preprocess)
        tock = time.time()
        print('Finished Indexing in', round(tock - tick, 0), 'seconds')

        # Create BM25 search index
        return self.get_bm25Okapi(self.df.index_tokens)

    def search(self,
               search_string: str,
               num_results=10,
               covid_related=False):

        search_terms = self._preprocess(search_string)
        doc_scores = self.bm25_index.get_scores(search_terms)

        # Get the index from the doc scores
        ind = np.argsort(doc_scores)[::-1]
        results = self.df.iloc[ind].copy()
        results['Score'] = doc_scores[ind].round(1)

        # Filter covid related
        if covid_related:
            results = results[results.covid_related]

        # Show only up to n_results
        results = results.head(num_results)

        # Create the final results
        results = results.drop_duplicates(subset=['title'])

        # Return Search Results
        return results



@lru_cache()
def get_root_dir() -> str:
    path = Path().cwd()
    while Path(path, "__init__.py").exists():
        path = path.parent
    return str(path)


def after(df:pd.DataFrame, date, include_null_dates=False):
    cond = df.publish_time >= date
    if include_null_dates:
        cond = cond | df.publish_time.isnull()
    return df[cond]


def since_sarscov2(df: pd.DataFrame, include_null_dates=False):
    return after(df, SARS_COV_2_DATE, include_null_dates)
