# incognito
# WORK IN PROGRESS###################
#TO Run
Execute Main Object with following arguments
 spark.master, no.of.cores, worker.memory, /input/path/folder/ [hdfs/tachyon/localdfs], input.filename
 
 Note: the input.filename, should also have a input.filename.taxonomy file describing the taxonomy of categorical data.
 		An example data set is under the data folder
 	Example
 		spark://master:1234
 		1
		256m
		/home/antorweep/Documents/data/
		data.dat

P.S. the Buckatization & Dichotomization phases respectively creates their own temp files 
		under /input/path/folder/out/buckets & under /input/path/folder/out/ec 
		these folders ../buckets and ../ecs needs to be deleted prior to 2nd run
		
#Solution Description
	1. Incognito
		1.1 Buckatization OK
		1.2 Dichotomization OK
		1.3 Redistribution OK 
		1.4 Recording OK 
	2. beta-likeness: 
		2.1 Buckatization: OK
		2.2 - 2.4: same as incognito: OK
	3. t-closeness
		3.1 Buckatization: OK
		3.2 Extension of Dichotomization from incognito: OK  
		3.3 - 3.4: same as incognito: OK
	TODO: Code documentation
		
#The following experiments evaluates the solution
	1. Information Loss: Imp. OK
	2. Similarity & Skewedness Attacks: Imp. OK
	3. Accuracy on anonymized data  
    	3.1 Association Rule Mining
    	3.2 Classification
	4. Performance evaluation: 15-20 machines for simulated data with respect the number of allocated cores	
				 
