rtnorm <- function(n, mean, sd, a = -Inf, b = Inf){
  rnorm(runif(n, pnorm(a, mean, sd), pnorm(b, mean, sd)), mean, sd)
}


#max<-30162
sampleSize <- 30162
saIndex<-9

set.seed(12345)
##Normal
#dist<-round(rtnorm(sampleSize, 50, 5, 10, 100))
##skewedDist
#l-r skewed
distl<-round(rbeta(sampleSize/2, 2, 25) * 100)
distr<-round(rbeta(sampleSize/2, 25, 2) * 100)
dist<-append(distl, distr) 
#r-skewed
#dist <- round(rbeta(sampleSize/2, 25, 2) * 100)
#l-skewed
#dist <- round(rbeta(sampleSize/2, 2, 25) * 100)

hist(dist)
min(dist)
max(dist)

file<-"/home/antorweep/workspace/incognito/data/adult.csv"
data<-read.csv(file, header = FALSE, sep = ",")
data<-cbind(data[sample(1:nrow(data), sampleSize),-9], dist)
head(data)

nrow(unique(data))

outFileName<-"adult.dat"
fileOut<-paste("/home/antorweep/workspace/incognito/data/adultSkewed", outFileName, sep="")
write.table(data, fileOut, row.names=F, col.names=F, quote=F, sep = ",")

#Add to taxonomy
sort(unique(dist))
hist(rbeta(10000,5,2))
hist(rbeta(10000,2,5))
hist(rbeta(10000,5,5))
