# This code can be put in any Python module, it does not require IPython
# itself to be running already.  It only creates the magics subclass but
# doesn't instantiate it yet.
from __future__ import print_function
import os
import pandas as pd
import pylab as pl
from IPython import display
from io import StringIO
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment, StreamTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.udf import udf
from pyflink.table.expressions import call 
from pyflink.table.udf import ScalarFunction
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
 
    

class MLflowFunction(ScalarFunction):
    def __init__(self, model_path):
        import mlflow.pyfunc
        self.model = mlflow.pyfunc.load_model(model_path)

    def eval(self, *argv):
        res=self.model.predict(list(argv))
        return res[0]
    
@magics_class
class FlinkPrintMagics(Magics):
 
    def __init__(self, shell):
        super(FlinkPrintMagics, self).__init__(shell)
        self.st_env = None
        self.t_env = None

    @line_magic
    def flink_init_batch_env(self, line):
        exec_env = ExecutionEnvironment.get_execution_environment()
        exec_env.set_parallelism(1)
        t_config = TableConfig()
        self.t_env = BatchTableEnvironment.create(exec_env, t_config)      
        
    @line_magic
    def flink_init_stream_env(self, line):
        self.s_env = StreamExecutionEnvironment.get_execution_environment()
        self.s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        self.s_env.set_parallelism(1)
        
        self.st_env = StreamTableEnvironment \
            .create(self.s_env, environment_settings=EnvironmentSettings
                    .new_instance()
                    .in_streaming_mode()
                    .use_blink_planner().build()) 
 
    @line_magic
    def flink_mlflow(self, line):
        if not self.st_env:
            print('Please initialize environment using %flink_init_stream_env')
            return
            
        shell = self.shell
        args=line.split()
        
        if len(args) < 4:
            print('Please use with parameters %flink_mlflow <function_name> <mlflow_model_path> <input_types> <result_type>')
            return
        
        args = [a.replace('"','') for a in args]
        function_name=args[0]
        model_path=args[1]
        shell.ex("from pyflink.table import DataTypes")
        input_types=shell.ev(args[2])
        result_type=shell.ev(args[3])
        mlflow_function = udf(MLflowFunction(model_path), input_types=input_types, result_type=result_type)
        self.st_env.register_function(function_name,mlflow_function)
        print(f'Function {function_name} registered')

    @line_magic
    def flink_register_function(self, line):
        if not self.st_env:
            print('Please initialize environment using %flink_init_stream_env')
            return
            
        shell = self.shell
        args=line.split()
        function_name=args[0]
        function_obj=shell.user_ns[args[1]]
        self.st_env.register_function(function_name,function_obj)
        print(f'Function {function_name} registered')

    @cell_magic
    def flink_execute_sql(self, line, cell):
        if not self.st_env:
            print('Please initialize environment using %flink_init_stream_env')
            return
        
        cell = cell.replace('\n',' ')
        self.st_env.execute_sql(cell)
        
    @cell_magic
    def flink_sql_query(self, line, cell):
        if not self.st_env:
            print('Please initialize environment using %flink_init_stream_env')
            return
        
        shell = self.shell
        cell = cell.replace('\n',' ')
        table_result2=self.st_env.sql_query(cell).execute()

        columns=table_result2.get_table_schema().get_field_names()
        df = pd.DataFrame(columns=columns)
        
        with table_result2.collect() as results:
            for result in results:
                res=[col for col in result]
                a_series = pd.Series(res, index=df. columns)
                display.clear_output(wait=True)
                df = df.append(a_series, ignore_index=True)
                display.display(df)
                
    @cell_magic
    def flink_sql_query_pie(self, line, cell):
        if not self.st_env:
            print('Please initialize environment using %flink_init_stream_env')
            return
        
        shell = self.shell
        cell = cell.replace('\n',' ')
        table_result2=self.st_env.sql_query(cell).execute()

        columns=table_result2.get_table_schema().get_field_names()
        df = pd.DataFrame(columns=columns)
        
        with table_result2.collect() as results:
            for result in results:
                res=[col for col in result]
                a_series = pd.Series(res, index=df. columns)
                display.clear_output(wait=True)
                df = df.append(a_series, ignore_index=True)
                
                df.groupby('smstype').size().reset_index(name='Count').set_index('smstype').plot.pie(y='Count', figsize=(5, 5))
                display.clear_output(wait=True)
                display.display(pl.gcf())       
        
    
# registration
 
def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    ipython.register_magics(FlinkPrintMagics)
