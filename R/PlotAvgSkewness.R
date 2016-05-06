i<-c(104.29, 55.47, 52.57, 33.37, 3.79, 0.0)
b<-c(214.55, 78.11, 65.09, 41.01, 33.70, 0.0)
t<-c(277.74, 73.83, 61.36, 30.84, 8.70, 0.0)

plot(i, 
     xaxt="n",
     
     type = "l",  
     xlab="Taxonomy Levels", ylab="%", 
     ylim=c(0, max(i,b,t, na.rm=TRUE)), col="red", sub="(a)")
axis(1, at=seq(1,length(i)), labels=seq(5,0))
lines(b, col="green")
lines(t, col="blue")
reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}
reset()
legend("top", legend=c("incognito (beta=3.0)", "beta (beta=3.0)", "tclose (t=0.804)"), fill=c("red", "green", "blue"), cex=0.75, ncol=3, bty="n")

