from typing import Tuple

from pyspark.sql.dataframe import DataFrame as SparkDataframe
from pyspark.sql.functions import col, isnull, when, length, lit, coalesce, regexp_replace, to_date

SARS_COV_2_DATE = '2019-11-30'
COLUMNS = ('publish_time', 'title', 'abstract', 'covid_related', 'virus_related',
           'corona_related', 'sars_related', 'url')


def clean_title(spark_df: SparkDataframe, relevant_regex: str = None) -> SparkDataframe:
    """
    Cleans and removes junk titles, which are filtered according to the regex provided
    :param spark_df: Spark Dataframe
    :param relevant_regex: Regex that is used to filter relevant titles
    :return: Filtered Dataframe
    """
    if relevant_regex is None:
        relevant_regex = '.*vir.*|.*sars.*|.*mers.*|.*corona.*|.*ncov.*|.*immun.*|.*nosocomial.*'
        relevant_regex = relevant_regex + '.*epidem.*|.*emerg.*|.*vacc.*|.*cytokine.*'

    is_title_junk = (length(col('title')) < 30) & ~(col('title').rlike(relevant_regex))
    spark_df = spark_df.na.fill('', ['title'])
    spark_df = spark_df.withColumn('title', when(is_title_junk, lit('')).otherwise(col('title')))
    return spark_df


def clean_abstract(spark_df: SparkDataframe, abstract_regex: str = None) -> SparkDataframe:
    """
    Removes unused words and filters abstracts
    :param spark_df: Spark Dataframe
    :param abstract_regex: Regex of words to be removed from abstract
    :return: Spark Dataframe
    """
    if abstract_regex is None:
        abstract_regex = '(Publisher|Abstract|Summary|BACKGROUND|INTRODUCTION)'

    spark_df = spark_df.withColumn('abstract',
                                   when(col('abstract') == 'Unknown', lit(None))
                                   .otherwise(col('abstract')))
    spark_df = spark_df.withColumn('abstract', coalesce('abstract', 'title'))
    spark_df = spark_df.withColumn('abstract', regexp_replace('abstract', abstract_regex, ''))
    spark_df = spark_df.drop_duplicates(['abstract'])
    return spark_df


def fill_nulls(spark_df: SparkDataframe,
               columns=['authors', 'doi', 'journal', 'abstract']) -> SparkDataframe:
    """
    Fill columns with null values
    :param spark_df: Spark Dataframe
    :return: Spark Dataframe with empty string instead of None
    """
    return spark_df.na.fill('', columns)


def tag_covid(spark_df: SparkDataframe,
              date_column: str = 'publish_time',
              abstract_column: str = 'abstract',
              covid_terms: str = None):
    """
    Creates a boolean column to flag whether the article is about COVID
    :param spark_df: Spark Dataframe
    :param date_column: Column with date of article
    :param abstract_column: Abstract Column
    :param covid_terms: List of covid terms to be used as a regular expression
    :return: Spark Dataframe with new boolean column
    """
    if covid_terms is None:
        covid_terms = ['covid', 'sars-?n?cov-?2', '2019-ncov', 'novel coronavirus', 'sars coronavirus 2']

    covid_search = f".*({'|'.join(covid_terms)})"

    since_covid = ((col(date_column) > SARS_COV_2_DATE) | (isnull(col(date_column))))
    covid_term_match = since_covid | col(abstract_column).rlike(covid_search)
    wuhan_outbreak = since_covid & col(abstract_column).rlike('.*(wuhan|hubei)')
    covid_match = covid_term_match | wuhan_outbreak
    spark_df = spark_df.withColumn('covid_related', coalesce(covid_match, lit(False)))
    return spark_df


def tag_virus(spark_df: SparkDataframe,
              abstract_column='abstract',
              virus_search: str = None) -> SparkDataframe:
    """
    Creates a new column that indicates whether the article is about viruses
    :param spark_df: Spark Dataframe
    :param abstract_column: Text column to search for references
    :param virus_search: regular expression to be applied
    :return: Dataframe with new boolean column indicating presence of virus terms
    """
    if virus_search is None:
        virus_search = f".*(virus|viruses|viral)"
    viral_cond = col(abstract_column).rlike(virus_search)
    return spark_df.withColumn('virus_related', coalesce(viral_cond, lit(False)))


def tag_coronavirus(spark_df: SparkDataframe, abstract_column='abstract', corona_regex: str = None):
    """
    Creates a column that indicates whether the article is about coronavirus
    :param spark_df: Spork Dataframe
    :param abstract_column: Text column to search for terms
    :param corona_regex: Regular expression to be applied
    :return: Dataframe with new boolean column indicating presence of coronavirus terms
    """
    if corona_regex is None:
        corona_regex = col(abstract_column).rlike(".*corona")
    return spark_df.withColumn('corona_related', coalesce(corona_regex, lit(False)))


def tag_sars(spark_df: SparkDataframe, abstract_column='abstract', sars_regex: str = None):
    """
    Creates a column that indicates whether the article is about sars
    :param spark_df: Spork Dataframe
    :param abstract_column: Text column to search for terms
    :param sars_regex: Regular expression to be applied
    :return: Dataframe with new boolean column indicating presence of coronavirus terms
    """
    if sars_regex is None:
        sars_regex = ".*sars"

    sars_cond = col(abstract_column).rlike(sars_regex)
    sars_not_covid = ~(col('covid_related')) & (sars_cond)
    return spark_df.withColumn('sars_related', coalesce(sars_not_covid, lit(False)))


def format_date(spark_df: SparkDataframe, date_column='publish_time') -> SparkDataframe:
    """
    Formats date column to a single format
    :param spark_df: Spark Dataframe
    :param date_column: Column to be formatted
    :return: Dataframe with formatted dates column
    """
    spark_df = spark_df.withColumn(date_column, when(isnull(col(date_column)), '').
                                   otherwise(to_date(col(date_column))))
    return spark_df


def run_pipeline(df: SparkDataframe,
                 columns: Tuple = COLUMNS,
                 abstract_column: str = 'abstract',
                 date_column: str = 'publish_time',
                 relevant_regex: str = None,
                 abstract_regex: str = None,
                 covid_terms: str = None,
                 virus_search: str = None,
                 corona_regex: str = None,
                 sars_regex: str = None) -> SparkDataframe:

    df = clean_title(spark_df=df, relevant_regex=relevant_regex)
    df = clean_abstract(spark_df=df, abstract_regex=abstract_regex)
    df = fill_nulls(spark_df=df)
    df = tag_covid(spark_df=df,
                   date_column=date_column,
                   abstract_column=abstract_column,
                   covid_terms=covid_terms)
    df = tag_virus(spark_df=df, abstract_column=abstract_column, virus_search=virus_search)
    df = tag_coronavirus(spark_df=df, abstract_column=abstract_column, corona_regex=corona_regex)
    df = tag_sars(spark_df=df, abstract_column=abstract_column, sars_regex=sars_regex)
    df = format_date(spark_df=df, date_column=date_column)
    df = df.select(list(columns))
    return df
