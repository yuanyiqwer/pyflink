import  pyflink as fk
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config=TableConfig()
t_env=BatchTableEnvironment.create(exec_env,t_config)
# 连接器输入
t_env.connect(FileSystem().path("./data/in")).with_format(OldCsv().field("word",DataTypes.STRING()))\
    .with_schema(Schema().field('word',DataTypes.STRING()))\
    .create_temporary_table('mySource')
t_env.connect(FileSystem().path("./data/out"))\
    .with_format(OldCsv().field_delimiter('\t').field('word',DataTypes.STRING()).field('count',DataTypes.BIGINT()))\
    .with_schema(Schema().field('word',DataTypes.STRING()).field('count',DataTypes.BIGINT()))\
    .create_temporary_table("mySink")
my_source_ddl = """
    create table mySource (
        word VARCHAR
    ) with (
        'connector.type' = 'filesystem',
        'format.type' = 'csv',
        'connector.path' = './data/in/in.csv'
    )
"""

my_sink_ddl = """
    create table mySink (
        word VARCHAR,
        `count` BIGINT
    ) with (
        'connector.type' = 'filesystem',
        'format.type' = 'csv',
        'connector.path' = './data/out'
    )
"""

t_env.sql_update(my_source_ddl)
t_env.sql_update(my_sink_ddl)
t_env.from_path('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('mySink')
t_env.execute("tutorial_job")


