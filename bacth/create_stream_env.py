from pyflink.table import *
import apache_beam as be
# 1.create a blink stream tableEnv
# env_stream_blink=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()

# 2.create a blink batch tableEnv
env_batch_blink=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()

# 3.create a flink stream tableEnv
# use_any_planner() 使用所有的执行计划（如果配置有的话），默认old
# env_stream_flink=EnvironmentSettings.new_instance().in_streaming_mode().use_any_planner().build()


# 4.create a flink batch tableEnv
# env_batch_fink=EnvironmentSettings.new_instance().in_batch_mode().use_old_planner().build()


# create table env
t_env=BatchTableEnvironment.create(environment_settings=env_batch_blink)
# table=t_env.from_elements([(1,'hi'),(2,'helo')],['id','data'])
table=t_env.from_elements([(1,'hi'),(2,'hello')],
    DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()), DataTypes.FIELD("data",DataTypes.STRING())]))
# table = t_env.from_elements([(1, 'Hi'), (2, 'Hello')],
#                                 DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
#                                                DataTypes.FIELD("data", DataTypes.STRING())]))
print(table.to_pandas()["id"].dtype)
print(table.to_pandas())



