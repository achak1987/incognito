install.packages("ISLR")
library("ISLR")
data(Wage)
head(Wage)

dat<-Wage[, c(-1, -7, -11)]
sal<-round(dat$wage)
dat<-cbind(dat[, -9], sal)
pids<-seq(1:nrow(dat))
dat<-cbind(pids, dat)

head(dat)
barplot(prop.table(table(dat[,9])))

fileOut<-"/home/antorweep/workspace/incognito/data/ISLR.Wage/ISLR.Wage"
write.table(dat, fileOut, row.names=F, col.names=F, quote=F, sep = ",")
