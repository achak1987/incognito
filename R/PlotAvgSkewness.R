i<-c(104.29, 55.47, 52.57, 33.37, 3.79, 0.0)
b<-c(214.55, 78.11, 65.09, 41.01, 33.70, 0.0)
t<-c(277.74, 73.83, 61.36, 30.84, 8.70, 0.0)

sum(i)
(sum(b)-sum(i))/sum(b)
(sum(t)-sum(i))/sum(t)


plot(i, 
     xaxt="n",
     type = "b",  
     xlab="Taxonomy Levels", ylab="%", 
     ylim=c(0, max(i,b,t, na.rm=TRUE)), col="red", pch=1, sub="(a)")
axis(1, at=seq(1,length(i)), labels=seq(5,0))
lines(b, col="green", type="b",pch=2)
lines(t, col="blue", type="b", pch=3)
reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}
reset()
legend("top", 
       legend=c("incognito (beta=3.0)", "beta (beta=3.0)", "tclose (t=0.804)"), 
       pch=c(1,2,3),
       col=c("red", "green", "blue"), cex=1, ncol=3, bty="n")

