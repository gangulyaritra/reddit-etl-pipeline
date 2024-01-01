import pandas as pd
from etls.reddit_etl import (
    reddit_connection,
    extract_posts,
    transform_data,
    load_data_to_csv,
)
from utils.constants import (
    REDDIT_CLIENT_ID,
    REDDIT_SECRET_KEY,
    REDDIT_USER_AGENT,
    OUTPUT_PATH,
)


def reddit_pipeline(file_name: str, subreddit: str, time_filter="day", limit=None):
    # Reddit Instance Connection.
    instance = reddit_connection(REDDIT_CLIENT_ID, REDDIT_SECRET_KEY, REDDIT_USER_AGENT)
    # Data Extraction.
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # Data Transformation.
    post_df = transform_data(post_df)
    # Data Loading into CSV.
    file_path = f"{OUTPUT_PATH}/{file_name}.csv"
    load_data_to_csv(post_df, file_path)

    return file_path
