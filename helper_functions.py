import numpy as np

def displayColumnsByGroup (df, columns_per_row, sample_size=5):
    
    total_columns=len(df.columns)
    group_size=total_columns//columns_per_row

    column_split=np.array_split(df.columns, group_size)

    for columns in column_split:
        df.select(*columns).show(sample_size)
        
def columnsLowerCase(df):
    return df.toDF(*[x.lower() for x in df.columns])