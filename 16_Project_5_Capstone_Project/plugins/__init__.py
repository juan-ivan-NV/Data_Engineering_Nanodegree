from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin


import operators
import helpers


class UdacityPlugin(AirflowPlugin):
    """
    Creating the pluging class to call the operators and helpers
    """
    
    name = "udacity_plugin"
    #         operators.SASValueToRedshiftOperator
    operators = [
        operators.CopyToRedshiftOperator,
        operators.DataQualityOperator
    ]
    """helpers = [
        helpers.sas_source_code_tables_data,
        helpers.copy_s3_keys
    ]"""