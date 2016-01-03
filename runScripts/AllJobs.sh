echo "STARTING ALL EXPERIMENTS"
echo "##############Incognito########################"
echo "[0mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m0/i o/*/ islr.dat 0,9 3.8 

echo "[1mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m1/i m1/*/ islr.dat 0,9 3.8

echo "[10mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m10/i m10/*/ islr.dat 0,9 3.8

echo "[100mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m100/i m100/*/ islr.dat 0,9 3.8

echo "[g1]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g1/i g1/*/ islr.dat 0,9 3.8

echo "[g10]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g10/i g10/*/ islr.dat 0,9 3.8

echo "[g20]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g20/i g20/*/ islr.dat 0,9 3.8

echo "[g30]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g30/i g30/*/ islr.dat 0,9 3.8

echo "[g40]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g40/i g40/*/ islr.dat 0,9 3.8

echo "[g50]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.IncognitoMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g50/i g50/*/ islr.dat 0,9 3.8

echo "##############Beta Main########################"
echo "[0mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m0/i o/*/ islr.dat 0,9 1.8 

echo "[1mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m1/i m1/*/ islr.dat 0,9 1.8

echo "[10mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m10/i m10/*/ islr.dat 0,9 1.8

echo "[100mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m100/i m100/*/ islr.dat 0,9 1.8

echo "[g1]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g1/i g1/*/ islr.dat 0,9 1.8

echo "[g10]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g10/i g10/*/ islr.dat 0,9 1.8

echo "[g20]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g20/i g20/*/ islr.dat 0,9 1.8

echo "[g30]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g30/i g30/*/ islr.dat 0,9 1.8

echo "[g40]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g40/i g40/*/ islr.dat 0,9 1.8

echo "[g50]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.BetaMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g50/i g50/*/ islr.dat 0,9 1.8

echo "##############TCloseness########################"
echo "[0mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m0/i o/*/ islr.dat 0,9 0.1 

echo "[1mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m1/i m1/*/ islr.dat 0,9 0.1

echo "[10mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m10/i m10/*/ islr.dat 0,9 0.1

echo "[100mb]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/m100/i m100/*/ islr.dat 0,9 0.1

echo "[g1]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g1/i g1/*/ islr.dat 0,9 0.1

echo "[g10]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g10/i g10/*/ islr.dat 0,9 0.1

echo "[g20]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g20/i g20/*/ islr.dat 0,9 0.1

echo "[g30]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g30/i g30/*/ islr.dat 0,9 0.1

echo "[g40]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g40/i g40/*/ islr.dat 0,9 0.1

echo "[g50]"
spark-1.6.0/bin/spark-submit \
--master spark://haisen26.ux.uis.no:7077  \
--executor-memory 25g \
--class uis.cipsi.incognito.examples.TCloseMain \
Incognito-1.0.jar \
-1 hdfs://haisen26.ux.uis.no:9000/1.6.0/islr_n/ hdfs://haisen26.ux.uis.no:9000/anonymized/1.6.0/islr_n/g50/i g50/*/ islr.dat 0,9 0.1

echo "FINISHED ALL EXPERIMENTS"
