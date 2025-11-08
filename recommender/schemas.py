import pandera as pa
from pandera.typing import Series

class RatingsSchema(pa.DataFrameModel):
    user_id: Series[int] = pa.Field(ge=1)
    movie_id: Series[int] = pa.Field(ge=1)
    rating: Series[int] = pa.Field(ge=1, le=5)
    
    
    ts: Series[int]