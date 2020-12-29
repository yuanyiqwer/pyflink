from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import apache_beam as be
# 1.create a tableEnv
env_settings=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env=StreamTableEnvironment.create(environment_settings=env_settings)

# 2.create source table
table_env.execute_sql("""
    create table datagen(
        id INT,
        data INT
    ) WITH(
        'connector'='datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '3'
    )
""")
# 3. create sink Table
table_env.execute_sql("""
    CREATE TABLE print (
        id INT,
        data INT
    ) WITH (
        'connector' = 'print'
    )
""")
# 4. query from source table and perform caculations
# create a Table from a Table API query:
# 加载数据源使用table api创建表
source_table=table_env.from_path("datagen")
source_table=table_env.sql_query("select * from datagen")
result_table=source_table.select("id+1,data")
# 5.使用table api执行插入
print("ss")
result_table.execute_insert("print").get_job_client().get_job_execution_result().result()
# table_env.execute_sql("INSERT INTO print SELECT * FROM datagen").get_job_client().get_job_execution_result().result()


#估计是随机值