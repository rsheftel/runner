import pandas as pd
import raccoon as rc


def rc_to_pd(raccoon_structure):
    """
    Convert a raccoon dataframe or series to pandas dataframe

    :param raccoon_structure: raccoon DataFrame or Series
    :return: pandas DataFrame
    """
    if isinstance(raccoon_structure, rc.DataFrame):
        data_dict = raccoon_structure.to_dict(index=False)
        return pd.DataFrame(data_dict, columns=raccoon_structure.columns, index=raccoon_structure.index)
    if isinstance(raccoon_structure, rc.Series):
        return pd.DataFrame({raccoon_structure.data_name: raccoon_structure.data}, index=raccoon_structure.index)


def pd_to_rc(pandas_dataframe, sort=None):
    """
    Convert a pandas dataframe to raccoon dataframe

    :param pandas_dataframe: pandas DataFrame
    :param sort: sort parameter to pass to raccoon DataFrame construction
    :return: raccoon DataFrame
    """
    columns = pandas_dataframe.columns.tolist()
    pandas_data = pandas_dataframe.to_numpy().T.tolist()
    data = {columns[i]: pandas_data[i] for i in range(len(columns))}
    index = pandas_dataframe.index.tolist()
    index_name = pandas_dataframe.index.name
    index_name = "index" if not index_name else index_name
    return rc.DataFrame(data=data, columns=columns, index=index, index_name=index_name, sort=sort)
