beta<-"0.7/skewness/data.csv"
i<-read.csv(paste("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/i/", beta, sep=""), header = F)
colnames(i)<-c("l5","l4","l3","l2","l1","l0")
i<-i[order(i$l5),] 

library(stringr)

par(mfrow = c(1, 5), mar=c(6,4,1,2))

#i, l5
l<-as.data.frame(table(i$l5))
sortBy<-as.integer(str_split_fixed(l[,1], "-", 2)[,1])
l<-cbind(l, sortBy)
l<-l[order(l[,3]),] 
plot(x=l[,3], y=l[,2], xaxt = "n", xlab="SAs at level #5", ylab="freq", type="l")
axis(1, at=l[,3], labels=l[,1])

#i, l4
l<-as.data.frame(table(i$l4))
sortBy<-as.integer(str_split_fixed(l[,1], "-", 2)[,1])
l<-cbind(l, sortBy)
l<-l[order(l[,3]),] 
plot(x=l[,3], y=l[,2], xaxt = "n", xlab="SAs at level #4", ylab="freq", type="h")
axis(1, at=l[,3], labels=l[,1])

#i, l3
l<-as.data.frame(table(i$l3))
sortBy<-as.integer(str_split_fixed(l[,1], "-", 2)[,1])
l<-cbind(l, sortBy)
l<-l[order(l[,3]),] 
plot(x=l[,3], y=l[,2], xaxt = "n", xlab="SAs at level #3", ylab="freq", type="h")
axis(1, at=l[,3], labels=l[,1])

#i, l2
l<-as.data.frame(table(i$l2))
sortBy<-as.integer(str_split_fixed(l[,1], "-", 2)[,1])
l<-cbind(l, sortBy)
l<-l[order(l[,3]),] 
plot(x=l[,3], y=l[,2], xaxt = "n", xlab="SAs at level #2", ylab="freq", type="h")
axis(1, at=l[,3], labels=l[,1])

#i, l1
l<-as.data.frame(table(i$l1))
sortBy<-as.integer(str_split_fixed(l[,1], "-", 2)[,1])
l<-cbind(l, sortBy)
l<-l[order(l[,3]),] 
plot(x=l[,3], y=l[,2], xaxt = "n", xlab="SAs at level #1", ylab="freq", type="h")
axis(1, at=l[,3], labels=l[,1])