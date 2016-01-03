library(scatterplot3d)
path <-"/home/antorweep/workspace/incognito/R/data/initialization/"
qis<-read.csv(paste(path,"qis", sep=""), header = FALSE )
qis<-qis[, c(3,4,5)]
head(qis)

#Works for upto 3-d data. 
seedLocale<-list.files(path = paste(path,"seeds", sep=""), full.names = TRUE, recursive = TRUE)


pdf(paste(path,"plot.pdf", sep=""))
for(i in 1:length(seedLocale)) { 
  sPath <- seedLocale[i]
  ctr <- read.csv(sPath, header = FALSE)
  
  if(ncol(qis) == 1) {
  plot(qis[,1])
  points(ctr[,1], col=c("red"), pch=2, lwd=2)  
  }
  else if(ncol(qis) == 2) {
    plot(qis[,1], qis[,2])
    points(ctr[,1], ctr[,2], col=c("red", "blue", "green", "orange", "pink", "violet"), pch=2, lwd=2)  
  }
  else {
    sp3d<-scatterplot3d(qis[,1], qis[,2], qis[,3], pch=1, lwd=.5, cex.symbols=0.1)
    sp3d$points3d(ctr[,1], ctr[,2], ctr[,3], col=c("red", "blue", "green", "orange", "pink", "violet"), pch=2, lwd=2)  
  }
}
dev.off()
