import pandas as pd
from scipy.sparse import csr_matrix

def create_user_item_matrix(df: pd.DataFrame) -> pd.DataFrame:
    user_item_matrix = df.pivot_table(
        index='user_id', columns='movie_id', values='rating'
    ).fillna(0)
    return user_item_matrix

def to_sparse_matrix(df: pd.DataFrame) -> csr_matrix:
    return csr_matrix(df.values)