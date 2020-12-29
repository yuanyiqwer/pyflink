from pyflink.table import *
import apache_beam as be

# 1.create a blink stream tableEnv

env_stream_blink=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env=StreamTableEnvironment.create(environment_settings=env_stream_blink)
table_env.execute_sql("""

""")