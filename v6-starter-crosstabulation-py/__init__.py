""" methods.py

This file contains all algorithm pieces that are executed on the nodes.
It is important to note that the master method is also triggered on a
node just the same as any other method.

When a return statement is reached the result is send to the central
server after encryption.
"""
import os
import sys
import time
import json
import pandas

from vantage6.tools.util import info, warn


def master(client, data, groupby_cols, results_col, organizations_to_include='ALL', subset=None):
    """
    Master algorithm to create a cross-tabulation of the federated datasets.
    
    Parameters
    ----------
    client : ContainerClient
        Interface to the central server. This is supplied by the wrapper.
    data : dataframe
        Pandas dataframe. This is supplied by the wrapper / node.
    groupby_cols : list
        List of columns to group the data on usings Pandas groupby() function. The rows of 
        the final cross-tabulated dataframe will be based on this.
    results_col : list
        List of one column name that will be used as outcome variable. The columns of the final 
        cross-tabulated dataframe will be based on this.
    organizations_to_include : list
        List of organizations id's to include for the statistics, or 'ALL' if
        you want to include all organizations of the collaboration.
    subset : dict
        Dictionary of columns you want to filter on (keys), and values you
        want to keep (values).
    Returns
    -------
    Dict
        A dictionairy containing summary statistics for all the columns of the
        dataset.
    """

    # define the input for the summary algorithm
    info("Defining input parameters")
    input_ = {
        "method": "cross_tabulation",
        "args": [],
        "kwargs": {
            "groupby_cols": groupby_cols,
            "results_col": results_col,
            "subset": subset
        }
    }

    # obtain organizations for which to run the algorithm
    if organizations_to_include=='ALL':
        organizations = client.get_organizations_in_my_collaboration()
        ids = [organization.get("id") for organization in organizations]
    else:
        ids = organizations_to_include

    # create node tasks
    info("Creating node tasks")
    task = client.create_new_task(
        input_,
        organization_ids=ids
    )

    results = wait_and_collect(client, task)

    if not results:
        return {'msg': 'Master container received empty results from RPC.'}

    for i, df_node in enumerate(results):
        if i==0:
            df_tmp = df_node
        else:
            df_tmp = df_tmp.add(df_node, fill_value=0)
    
    master_result = df_tmp

    info("master algorithm complete")

    return master_result

def RPC_cross_tabulation(dataframe, groupby_cols, results_col, subset=None):
    """
    Creates a cross-tabulated version of the dataframe.

    Parameters
    ----------
    dataframe : pandas dataframe
        Pandas dataframe that contains the local data.
    groupby_cols : list
        List of columns to group the data on usings Pandas groupby() function. The rows of 
        the final cross-tabulated dataframe will be based on this.
    results_col : list
        List of one column name that will be used as outcome variable. The columns of the final 
        cross-tabulated dataframe will be based on this.
    subset : Dictionairy
        Dictionary of columns you want to filter on (keys), and values you
        want to keep (values).
    Returns
    -------
    Dict
        A Dict containing some simple statistics for the local dataset.
    """

    if subset:
        dataframe = subset_data(dataframe, subset)

    dataframe = dataframe.fillna('N/A')
    cross_tab_df = dataframe.groupby(groupby_cols+results_col, dropna=False)[results_col].count().unstack(level=results_col[0]).copy()

    return cross_tab_df

def wait_and_collect(client, task):
    """
    Waits till the nodes are done with processing a task, and 
    collects the results.
    
    Parameters
    ----------
    client : ContainerClient
        Interface to the central server. This is supplied by the wrapper.
    task : Dictionary
        vantage6 task information obtained from client.create_new_task().

    Returns
    -------
    Dict
        A Dict with result dictionaries per node.
    """
# wait for all results
    # TODO subscribe to websocket, to avoid polling
    task_id = task.get("id")
    task = client.request(f"task/{task_id}")
    while not task.get("complete"):
        task = client.request(f"task/{task_id}")
        info("Waiting for results")
        time.sleep(1)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))

    return results

def subset_data(dataframe, subset):
    """
    Subsets, or filters the dataframe, based on the dictionary 'subset'.
    Parameters
    ----------
    dataframe : pandas dataframe
    subset : Dictionary
        Dictionary of which the keys point to columns you want to filter on,
        and values you want to keep.
        Example:
        subset = {'sex': ['Female'], 'treatment': [1,2,4]}
        With this subset we get the data for females that got either treatment 1, 2 or 4.
    Returns
    -------
    pandas dataframe
        The filtered dataframe.
    """
    for k,v in subset.items():
        dataframe = dataframe.loc[dataframe[k].isin(v)]
    print(f'Length of subsetted dataframe: {len(dataframe)}')

    return dataframe
