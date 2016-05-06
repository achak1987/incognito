bTime<-c(20,25,43,36,35,46,46,45,60,62,52,74,79,75,78,92,84,94,104,98,110,116,108,123,133.5, 135.5, 137.5,151,163,152,162,168,162)
eTime<-c(401,388,72,480,584,130,662,704,156,703,779,178,940,1015,222,908,998,213,1002.5,1096,177,1097,1107,255,1479,1556,250,1577,1835,245,1537,1943,248)
redTime<-c(34,34,57,168,162,1082,406,419,2595,694,792,4591,1263,1934,6689,1486,2658,12633,2138.5,2282,14668,2791,4165,21851,5149,8631,23438,5031,13513,44579,10171,18751,83329)
recTime<-c(10,12,14,61,65,232,246,235,196,569,548,356,883,903,663,1283,1069,1031,1624.5,1827,1243,1966,2707,2642,2054,5205,7526,9320,11053,12927,13990,22567,28473)
total<-bTime+eTime+redTime+recTime
(c(116,108,123) + c(151,163,152) )/2
#col1: Incognito, 2: beta, 3: tclose
bTime<-matrix(bTime, ncol = 3, byrow=T)
eTime<-matrix(eTime, ncol = 3, byrow=T)
redTime<-matrix(redTime, ncol = 3, byrow=T)
recTime<-matrix(recTime, ncol = 3, byrow=T)
total<-matrix(total, ncol = 3, byrow=T)


xLab<-c("1G","10G","20G","30G","40G","50G","60G","70G","80G","90G","100G")

length(bTime)
length(bTime)

layout(matrix(c(1,2,3,4,5,5), 3, 2, byrow = TRUE))

plot(x=seq(1,length(bTime[,1])), y=bTime[,1], 
     type = "l", xaxt="n", xlab="Data Sizes", ylab="Bucketization (in seconds)", 
     ylim=c(0, max(bTime, na.rm=TRUE)), col="red", sub="(a)")
axis(1, at=seq(1,length(bTime[,1])), labels=xLab)
lines(bTime[,2], col="green")
lines(bTime[,3], col="blue")

plot(x=seq(1,length(eTime[,1])), y=eTime[,1], 
     type = "l", xaxt="n", xlab="Data Sizes", ylab="Dichotomization (in seconds)", 
     ylim=c(0, max(eTime, na.rm=TRUE)), col="red", sub="(b)")
axis(1, at=seq(1,length(eTime[,1])), labels=xLab)
lines(eTime[,2], col="green")
lines(eTime[,3], col="blue")

plot(x=seq(1,length(redTime[,1])), y=redTime[,1], 
     type = "l", xaxt="n", xlab="Data Sizes", ylab="Redistribution (in seconds)", 
     ylim=c(0, max(redTime, na.rm=TRUE)), col="red", sub="(c)")
axis(1, at=seq(1,length(redTime[,1])), labels=xLab)
lines(redTime[,2], col="green")
lines(redTime[,3], col="blue")

plot(x=seq(1,length(recTime[,1])), y=recTime[,1], 
     type = "l", xaxt="n", xlab="Data Sizes", ylab="Recording (in seconds)", 
     ylim=c(0, max(recTime, na.rm=TRUE)), col="red", sub="(d)")
axis(1, at=seq(1,length(recTime[,1])), labels=xLab)
lines(recTime[,2], col="green")
lines(recTime[,3], col="blue")

plot(x=seq(1,length(total[,1])), y=total[,1], 
     type = "l", xaxt="n", xlab="Data Sizes", ylab="Total Execution (in seconds)", 
     ylim=c(0, max(total, na.rm=TRUE)), col="red", sub="(e)")
axis(1, at=seq(1,length(total[,1])), labels=xLab)
lines(total[,2], col="green")
lines(total[,3], col="blue")

reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}

reset()
legend("top", legend=c("incognito (beta=0.25)", "beta (beta=0.3)", "tclose (t=0.805)"), fill=c("red", "green", "blue"), cex=0.75, ncol=3, bty="n")

