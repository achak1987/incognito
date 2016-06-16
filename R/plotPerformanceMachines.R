#5 GB Total
i<-c(3382,938,637,568,714,704)
b<-c(3475,887,638,652,581,612)
t<-c(2223,596,494,407,391,398)
xLab<-c(1, 3, 5, 7, 9, 11)
plot(i, 
     xaxt="n",
     type = "b",  
     xlab="#Machines", ylab="Total Execution (in seconds)", 
     ylim=c(0, max(i,b,t, na.rm=TRUE)), pch=1, col="red", sub="(a)")
axis(1, at=seq(1,length(i)), labels=xLab)
lines(b, type="b", col="green", pch=2)
lines(t, type="b", col="blue", pch=3)
reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}
reset()
legend("top", legend=c("incognito (beta=3.0)", "beta (beta=3.0)", "tclose (t=0.804)"), 
       pch=c(1,2,3),
       col=c("red", "green", "blue"), cex=1, ncol=3, bty="n")