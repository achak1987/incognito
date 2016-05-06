i.beta<-read.csv("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/i/beta/data.csv")
b.beta<-read.csv("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/b/beta/data.csv")
t.t<-read.csv("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/t/t/data.csv")

colnames(i.beta) <- c("beta", "buckets", "ecs")
colnames(b.beta) <- c("beta", "buckets", "ecs")
colnames(t.t) <- c("t", "buckets", "ecs")


par(mfrow = c(2, 2), mar=c(6,4,1,2))
#T BK
plot(x=t.t$t, y=t.t$buckets, typ="l", col="blue", xlab = "t", ylab="#buckets", sub="(a)")
#T EC
plot(x=t.t$t, y=t.t$ecs, typ="l", col="blue", xlab = "t", ylab="#ecs", sub="(a)")
#I,B BUK
plot(x=i.beta$beta, y=i.beta$buckets, typ="l", col="red", 
     xlim=c(min(i.beta$beta, b.beta$beta),max(i.beta$beta, b.beta$beta)), 
     ylim=c(min(i.beta$buckets, b.beta$buckets),max(i.beta$buckets, b.beta$buckets)), xlab = "beta", ylab="#buckets", sub="(a)")
lines(x=b.beta[,1], y=b.beta[,2], typ="l", col="green")
#I,B EC
plot(x=i.beta$beta, y=i.beta$ecs, typ="l", col="red",
     xlim=c(min(i.beta$beta, b.beta$beta),max(i.beta$beta, b.beta$beta)), 
     ylim=c(min(i.beta$ecs, b.beta$ecs),max(i.beta$ecs, b.beta$ecs)), xlab = "beta", ylab="#ecs", sub="(a)")
lines(x=b.beta$beta, y=b.beta$ecs, typ="l", col="green")
reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}

reset()
legend("top", legend=c("incognito", "beta", "tclose"), fill=c("red", "green", "blue"), cex=0.75, ncol=3, bty="n")

#i: beta, b, e
#0.7     181  14  
#0.9     181  21
#7.1      66   7  
#b: beta, b, e
#0.7     180  19
#7.1      32   7
#t: t, b, e
#0.80       5   3
#0.81       5  21

cbind(i.beta, b.beta)
