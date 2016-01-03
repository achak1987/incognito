path<-"/home/antorweep/git/incognito/data/islr/"
filename<-"islr.dat.bk"
trfilename<-"train.islr.dat"
tefilename<-"test.islr.dat"
data<-read.csv(paste(path, filename, sep=""), header = F)
trainSize<- nrow(data) * 0.80
train_ <- sample(1:nrow(data),trainSize,replace=FALSE)
train <- data[train_,] # select all these rows
test <- data[-train_,] # select all but these rows
#Output the train & test filename
write.table(train, paste(path, trfilename, sep=""), row.names=F, col.names=F, quote=F, sep=",")
write.table(train, paste(path, tefilename, sep=""), row.names=F, col.names=F, quote=F, sep=",")

#MAKE SURE TO RENAME THE TRAIN DATASET TO THE ORIGINAL FILENAME, SO THAT THE NAMING STRUCTURE MATCHES WITH THE STRUCTURE & TAXONOMY FILES
#KEEP A BACK UP OF THE ORIGINAL FILE AS WELL

