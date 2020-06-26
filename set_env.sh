conda deactivate #deactivate any active conda environments
#deactivate # deactivate any python venvs

export SPARK_HOME=/usr/local/spark-3.0.0-bin-hadoop2.7/
export PATH=$SPARK_HOME/bin:$PATH

export PYSPARK_PYTHON=./venv/bin/python3.8

#export PYSPARK_DRIVER_PYTHON=jupyter
#export PYSPARK_DRIVER_PYTHON_OPTS='notebook'

# spark-submit --packages io.delta:delta-core_2.12:0.7.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  main.py

# pyspark --packages io.delta:delta-core_2.12:0.7.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"