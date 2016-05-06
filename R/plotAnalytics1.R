i.0.8.b.0.7<-rbind(c(23.37623,23.12436,23.22573,23.47555,18.84456),
                     c(29.15978,30.26931,32.71395,27.53945,30.14983),
                   c(29.62312,30.26931,29.23985,27.57481,30.16679))

i.0.9.t.0.81<-rbind(c(23.37623,23.12436,23.22573,23.47555,18.84456),
                   c(31.82565,30.26931,29.62021,31.80660,29.94559),
                   c(41.53160,33.74790,29.34520,36.68982,31.02757))

rownames(i.0.8.b.0.7)<-c("original","incognito","beta")
colnames(i.0.8.b.0.7)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.0.9.t.0.81)<-c("original","incognito","tclose")
colnames(i.0.9.t.0.81)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")

par(mfrow = c(1, 2), mar=c(6,4,1,2))
barplot(i.0.8.b.0.7, sub="(a) with i=0.8(#ecs=19), b=0.7(#ecs=19)", 
        beside=TRUE, col=c("black","red", "green"), cex.names=0.75)
barplot(i.0.9.t.0.81, sub="(b) with i=0.9(#ecs=21), t=0.81(#ecs=21)",
        beside=TRUE, col=c("black","red", "blue"), cex.names=0.75)
reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}

reset()
legend("top", legend=c("original", "incognito", "beta", "tclose"), fill=c("black", "red", "green", "blue"), cex=0.75, ncol=3, bty="n")
