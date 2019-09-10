from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class WeatherflyPlugin(AirflowPlugin):
    name = "weatherfly_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateTablesInRedshiftOperator,
        operators.AirportNameTranslate
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.CreateTables
    ]
