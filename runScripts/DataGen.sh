echo "####DATA GEN###################"
echo "start"
: <<'END'
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/islr_ori.dat hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/o 1 1 , 0,9
echo "original dataset created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/o/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/m1 1 2 , 0,9
echo "1m created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/m1/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/m10 1 10 , 0,9
echo "10m created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/m10/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/m100 1 10 , 0,9
echo "100mb created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/m100/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g1 1 10 , 0,9

echo "1gb created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g1/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g10 1 10 , 0,9

echo "10gb created"
END

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g10/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g20 1 2 , 0,9

echo "20gb created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g10/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g30 1 3 , 0,9

echo "30gb created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g20/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g40 1 2 , 0,9

echo "40gb created"

spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077 \
--executor-memory 25g \
--class uis.cipsi.incognito.utils.DataGen2 \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g10/*/ hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/g50 1 5 , 0,9

echo "50gb created"
