"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline, node
from sg_api.nodes.kedro_temperature_nodes import avg_temp_by_hour, get_temp_data, choose_station
from sg_api.pipelines import de_subnode_parallel_precossing
from sg_api.pipeline import create_pipelines

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    #pipelines = find_pipelines(create_pipelines)
    #pipelines["__default__"] = create_pipelines
    de = Pipeline([
        node(get_temp_data,
             inputs=None,
             outputs='raw_temp'),
        node(
            choose_station,
            inputs=['raw_temp', 'params:station_id'],
            outputs='station_temperature',
        )])

    ds = Pipeline([
        node(
            avg_temp_by_hour,
            inputs='station_temperature',
            outputs='temp_plot',
        )
    ])

    de_subnode = de_subnode_parallel_precossing.create_pipeline()

    return {
        "de_subnode": de_subnode,
        "de": de,
        "ds": ds,
        "__default__": de_subnode+ds,
    }
