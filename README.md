# Spark Anonymization Toolkit
 #######################WORK IN PROGRESS###################
 Data with only numeric values for Quasi Identifiers are processed.
 Sensitive Attributes are String type. They much contain a user defined taxonomy tree. Explained bellow.
  
 Currently the following algorithms are implemented. 
 	t-closeness
 	beta-likeness
 	incognito (our solution)

#Included example data set.
	ISLR.WAGE (with selected features)
	islr.dat - comma seperated dataset (PI: primary identifier, QI: quasi identifier, SA: sensitive attribute)
		PI id: Unique Number value for each record (custom inserted)
		QI year: numeric
		QI age: numeric
		QI maritl: numeric (A factor with levels 1. Never Married 2. Married 3. Widowed 4. Divorced and 5. Separated indicating marital status)
		QI race: numeric (A factor with levels 1. White 2. Black 3. Asian and 4. Other indicating race)
		QI education: numeric (A factor with levels 1. < HS Grad 2. HS Grad 3. Some College 4. College Grad and 5. Advanced Degree indicating education level)
		QI jobclass: numeric (A factor with levels 1. Industrial and 2. Information indicating type of job)
		QI health: numeric (A factor with levels 1. <=Good and 2. >=Very Good indicating health level of worker)
		QI health_ins: numeric (A factor with levels 1. Yes and 2. No indicating whether worker has health insurance)
		SA wage: Workers raw wage (represented as string with a taxonomy tree)
		
		total records 3000 records
		
	islr.dat.structure - describes the structure of the dataset. Has to be user defined.  Each entry represents a column. The filename should prefix with the filename of the dataset file that is describes
		index,isTypeNumeric=(0 False, 1 True),noOfLevels(0 numeric, n levels in the taxonomy tree of the string attribute)
		
	islr.dat.taxonomy - describes the taxonomy tree for the string columns. Has to be user defined. For this dataset includes the Sensitive Attribute taxonomy. The filename should prefix with the filename of the dataset file that is describes
		child,parent		
		Example:					
													Salary
					
					   0-99							       100-199 				...							
		
	     0-49				         50-99			100-149			150-199		
	
	0-9   ... 	40-49	 		50-59 ... 90-99		...
  0-4   5-9  40-44   45-49  ...
 3   2   8     43   45 48 49
 
 		In the above example set there are 5 Levels. The content of the file would be
 			3:0-4
 			2:0-4
 			0-4:0-9
 			0-9:0-49
 			0-49:0-99
 			0-99:Salary
 			Salary,*
 			*,*
 			...
 			0-99,Salary
 			
		
		
#TO Run
Currently the data needs to be converted to a Object format. We run the DataGen Object



spark-1.6.0/bin/spark-submit \
--master spark://spark.master:port \
--executor-memory memory \
--class uis.cipsi.incognito.utils.DataGen \
Incognito-1.0.jar \
-1 /home/.../islr/islr.dat /home/.../islr/o 1 1 , 0,9

[Arguments: 
	-1: used to set spark master when run from eclipse on local. Uncomment line 59 (sparkConf.setMaster(sparkMaster)) in uis.cipsi.incognito.rdd.CustomSparkContext if local is used instead from eclipse
	inPath: can be file or hdfs
	outPath: can be file or hdfs
	inSize: keep as 1
	outSize: keep as 1
	column seperator
	primaryIdentifierIndex,sensitiveAttributeIndex]
	
Once the Object file is created, we can run any of the anonymization methods as follows
Incognito
	spark-1.6.0/bin/spark-submit \
--master spark://spark.master:port  \
--executor-memory memory \
--class uis.cipsi.incognito.examples.Algorithm \
Incognito-1.0.jar \
-1 /home/.../islr/ /home/.../islr/anonymized/FileOutName o/*/ islr.dat 0,9 threshold numPartitions

*For Incognito: IncognitoMain, Beta-Likeness: BetaMain, T-Closeness: TCloseMain

[Arguments: 
	-1: used to set spark master when run from eclipse on local. Uncomment line 59 (sparkConf.setMaster(sparkMaster)) in uis.cipsi.incognito.rdd.CustomSparkContext if local is used instead from eclipse
	path: path where the data file is located (just the path without filename. can be file or hdfs)
	outPath: can be file or hdfs
	filename: filename of the dataset to be anonymized. Since we already created a object file we give the folder ex.: o/*/
	originalFilename: Since the structure and taxonomy files are named as originalfilename.taxonomy and original filename.structure. Since, the path is already specified as an earlier argument, only the file name is required
	primaryIdentifierIndex,sensitiveAttributeIndex
	a threshold value. Depends on the algorithm and data type. for incognito and beta it should be greater than 0. For proper results, it should be greater than atleast 1. For Tcloseness the value has to between 0 and 1. Alter the threshold to get a optimal number of Equvalance classes. Ensure that you have atleast more than 1 equivalance class]
 	numPartitions: usually 2 or 3 * total number of cores on your spark cluster
 	
 
				 
