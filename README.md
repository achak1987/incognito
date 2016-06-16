# Incognito - a large scale anonymization framework
*WORK IN PROGRESS*

Primary Attribute can be either numeric or String
	
* TODO: Multiple Primary Attributes

Data with only Numeric Values for Quasi Identifiers are processed.
	
* TODO: Quasi Identifiers of String Type

Sensitive Attributes are String type. They must contain a user defined taxonomy tree. Explained bellow.
	
* TODO: Multiple Sensitive Stributes
  
 Currently the following algorithms are implemented. 
 
 	* [t-closeness](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=4221659&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D4221659)
 	* [beta-likeness](http://dl.acm.org/citation.cfm?id=2350255)
 	* incognito (under review)
 	* [Mondrian](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=1617393&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D1617393)
 		
* TODO: Partitioning for the Equivalence Class creation process


#Included example data set

	* ISLR.WAGE (with selected features): https://cran.r-project.org/web/packages/ISLR/ISLR.pdf
	* islr.dat - comma seperated dataset (PI: primary identifier, QI: quasi identifier, SA: sensitive attribute)
		- PI id: Unique Number value for each record (custom inserted)
		- QI year: numeric
		- QI age: numeric
		- QI maritl: numeric (A factor with levels 1. Never Married 2. Married 3. Widowed 4. Divorced and 5. Separated indicating marital status)
		- QI race: numeric (A factor with levels 1. White 2. Black 3. Asian and 4. Other indicating race)
		- QI education: numeric (A factor with levels 1. < HS Grad 2. HS Grad 3. Some College 4. College Grad and 5. Advanced Degree indicating education level)
		- QI jobclass: numeric (A factor with levels 1. Industrial and 2. Information indicating type of job)
		- QI health: numeric (A factor with levels 1. <=Good and 2. >=Very Good indicating health level of worker)
		- QI health_ins: numeric (A factor with levels 1. Yes and 2. No indicating whether worker has health insurance)
		- SA wage: Workers raw wage (represented as string with a taxonomy tree)
		
		- total records 3000 records
	* islr.dat.structure - describes the structure of the dataset. 
		- Has to be user defined.
		- Each entry represents a column. 
		- The **filename** should **prefix** with the filename of the dataset file that is describes
		- Syntex: index,isTypeNumeric=(0 False, 1 True),noOfLevels(0 numeric, n levels in the taxonomy tree of the string attribute)
	* islr.dat.taxonomy - describes the taxonomy tree for the string columns. 
		- Has to be user defined. 
		- This dataset includes the Sensitive Attribute taxonomy. 
		- The filename should prefix with the filename of the dataset file that is describes child,parent
	
	
	Example:					
```
													Salary
					
					   0-99							       100-199 				...							
		
	     0-49				         50-99			100-149			150-199		
	
	0-9   ... 	40-49	 		50-59 ... 90-99		...
  0-4   5-9  40-44   45-49  ...
3    2   8    43    45 48 49 ...
 ```
 
 	In the above example set there are 5 Levels. The content of the file would be
 ```
 3,0-4
 2,0-4
 ...
 0-4,0-9
 ...
 0-9,0-49
 ...
 0-49,0-99
 ...
 0-99,Salary
 ...
 Salary,*
 *,*
 ...
 ``` 			
 
 
#TO Run

**incognito, tcloseness and beta likeness**

Determine the optimal threshold and the number of buckets and equivalence classes that cab be created for a given dataset. Choose an threshold that creates a balanced number of buckets and equivalence classes-

*The output is listed as follows:*

threshold, bucketCount, equivalenceClassCount, bucketizationTime, dichotomizationTime
```
spark-1.6.0/bin/spark-submit \
--master spark://spark.master:port \
--executor-memory memory \
--class incognito.utils.BucketECThesholdList \
Incognito-1.0.jar \
-1 InputFolderPath  dataset.filename columnSeperator primaryIdentifierIndex,SensitiveAttributeIndex \
startTreshold stopThreshold incrementBy algorithmName numPartitions
```
 
```
[Arguments: 
- -1: used to set spark master. Only required, when running from IDE
- InputFolderPath: can be file or hdfs. Path where the data set to be anonymized, its structure and taxonomy is located
- dataset.filename: filename of the dataset to be anonymized
- columnSeperator: a seperator string that seperates column values in the the input file
- primaryIdentifierIndex,SensitiveAttributeIndex: the index of the primary and the sensitive attribute (starts with **zero**)
- startTreshold: start with 0. Specifies the starting threshold
- stopThreshold: for *tcloseness*, specify the maximum value as 1. For *beta-likeness, incognito* it can be any number. Recommanded to use *3*, *5*, or *10*.
- incrementBy: determines, by how much each itteration should increment the threshold. Recommanded to use *0.1*
- algorithmName: Specifies the algorithm to run. Use any one of the values: *incognito* or *beta* or *tclose*
- numPartitions: usually 2 or 3 * total number of cores on your spark cluster]
```	

Once a optimal threshold for is determined for the algorithm, run the following to anonymize the dataset
```
spark-1.6.0/bin/spark-submit \
--master spark://spark.master:port \
--executor-memory memory \
--class incognito.examples.PhaseBasedAnonymization \
Incognito-1.0.jar \
-1 InputFolderPath  dataset.filename columnSeperator primaryIdentifierIndex,SensitiveAttributeIndex \
treshold numPartitions outputPath algorithmName
```
```
[Arguments: 
- -1: used to set spark master. Only required, when running from IDE
- InputFolderPath: can be file or hdfs. Path where the data set to be anonymized, its structure and taxonomy is located
- dataset.filename: filename of the dataset to be anonymized
- columnSeperator: a seperator string that seperates column values in the the input file
- primaryIdentifierIndex,SensitiveAttributeIndex: the index of the primary and the sensitive attribute (starts with **zero**)
- treshold: the optimal treshold to run the algorithm with
- numPartitions: usually 2 or 3 * total number of cores on your spark cluster
- outputPath: the output path where the anonymized data set is to be saved
- algorithmName: Specifies the algorithm to run. Use any one of the values: *incognito* or *beta* or *tclose*]
```
 	
**Mondrian** a is simple k-anonymization technique.
 ```
 spark-1.6.0/bin/spark-submit \
--master spark://spark.master:port  \
--executor-memory memory \
--class incognito.examples.MondrianMain \
Incognito-1.0.jar \
-1 InputFolderPath/dataset.filename primaryIdentifierIndex,SensitiveAttributeIndex K numPartitions columnSeperator
 ```
 
 ```
[Arguments: 
- -1: used to set spark master. Only required, when running from IDE
- InputFolderPath/dataset.filename: can be file or hdfs. Path where the data set to be anonymized, its structure and taxonomy is located/filename of the dataset to be anonymized
- primaryIdentifierIndex,SensitiveAttributeIndex: the index of the primary and the sensitive attribute (starts with **zero**)
- K: min sizes of anonymized groups
- numPartitions: usually 2 or 3 * total number of cores on your spark cluster
- columnSeperator: a seperator string that seperates column values in the the input file]
```

**Developed for apache spark 1.6.0 and onwards**
