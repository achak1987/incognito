require(plotrix)
source("/home/antorweep/git/SparkAnonymizationToolkit/R/barp2.R")

i.b.0.7.t.0.8<-rbind(c(23.37623,23.12436,23.22573,23.47555,18.84456),
c(32.61873,30.26931,28.82740,32.61232,30.09025),
c(29.62312,30.26931,29.23985,26.90695,30.15420),
c(78.41589,30.64841,43.68714,43.00570,30.34276))

i.b.0.7.t.0.81<-rbind(c(23.37623,23.12436,23.22573,22.67933,18.73048),
c(32.61873,30.26931,28.82740,32.61499,30.08007),
c(29.62312,30.26931,29.23985,27.57481,30.16679),
c(41.53160,33.74790,29.34520,36.69406,31.06287))

i.b.t.0.8<-rbind(c(23.37623,23.12436,23.22573,23.47167,18.75763),
c(29.15978,30.26931,32.71395,27.53945,30.14983),
c(43.93379,30.26931,27.47673,43.93920,30.24089),
c(78.41589,30.64841,43.68714,43.02927,30.36033))

i.b.0.8.t.0.81<-rbind(c(23.37623,23.12436,23.22573,23.48439,18.71291),
c(29.15978,30.26931,32.71395,27.51309,30.06303),
c(43.93379,30.26931,27.47673,43.92933,30.22519),
c(41.53160,33.74790,29.34520,36.68982,31.02757))

i.b.0.9.t.0.8<-rbind(c(23.37623,23.12436,23.22573,23.47982,18.80329),
c(31.82565,30.26931,29.62021,31.80689,29.98619),
c(45.89011,30.26931,65.97833,45.83422,30.27095),
c(78.41589,30.64841,43.68714,43.02923,30.36934))

i.b.0.9.t.0.81<-rbind(c(23.37623,23.12436,23.22573,23.47558,18.79738),
c(31.82565,30.26931,29.62021,31.80736,29.93669),
c(45.89011,30.26931,65.97833,45.83096,30.29127),
c(41.53160,33.74790,29.34520,36.66352,30.99359))

i.b.7.1.t.0.8<-rbind(c(23.37623,23.12436,23.22573,21.82264,18.80139),
c(72.58365,28.77988,28.22911,72.55395,28.80064),
c(29.29520,30.34985,29.53223,29.28859,30.24206),
c(78.41589,30.64841,43.68714,43.02920,30.36385))

i.b.7.1.t.0.81<-rbind(c(23.37623,23.12436,23.22573,23.47988,18.78589),
c(72.58365,28.77988,28.22911,72.55656,28.81488),
c(29.29520,30.34985,29.53223,29.28807,30.25771),
c(41.53160,33.74790,29.34520,36.66721,31.06398))

rownames(i.b.0.7.t.0.8)<-c("original","incognito","beta","tclose")
colnames(i.b.0.7.t.0.8)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.b.0.7.t.0.81)<-c("original","incognito","beta","tclose")
colnames(i.b.0.7.t.0.81)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.b.t.0.8)<-c("original","incognito","beta","tclose")
colnames(i.b.t.0.8)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.b.0.8.t.0.81)<-c("original","incognito","beta","tclose")
colnames(i.b.0.8.t.0.81)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.b.0.9.t.0.8)<-c("original","incognito","beta","tclose")
colnames(i.b.0.9.t.0.8)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.b.0.9.t.0.81)<-c("original","incognito","beta","tclose")
colnames(i.b.0.9.t.0.81)<-c("ln.reg.","dec.tr","reg.","nnet","rnd.frst")
rownames(i.b.7.1.t.0.8)<-c("original","incognito","beta","tclose")
colnames(i.b.7.1.t.0.8)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")
rownames(i.b.7.1.t.0.81)<-c("original","incognito","beta","tclose")
colnames(i.b.7.1.t.0.81)<-c("ln.reg.","dec.tr","knn.reg.","nnet","rnd.frst")

par(mfrow = c(4, 2), mar=c(6,4,1,2))
barplot(i.b.0.7.t.0.8, sub="(a) with i=0.7(#ecs=14), b=0.7(#ecs=19), t=0.8(#ecs=3)", 
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.0.7.t.0.81, sub="(b) with i=0.7(#ecs=14), b=0.7(#ecs=19), t=0.81(#ecs=21)",
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.t.0.8, sub="(c) with i=0.8(#ecs=19), b=0.8(#ecs=9), t=0.8(#ecs=3)", 
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.0.8.t.0.81, sub="(d) with i=0.8(#ecs=19), b=0.8(#ecs=9), t=0.81(#ecs=21)",
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.0.9.t.0.8, sub="(e) with i=0.9(#ecs=21), b=0.9(#ecs=3), t=0.8(#ecs=3)",
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.0.9.t.0.81, sub="(f) with i=0.9(#ecs=21), b=0.9(#ecs=3), t=0.81(#ecs=21)",
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.7.1.t.0.8, sub="(g) with i=7.1(#ecs=7), b=7.1(#ecs=7), t=0.8(#ecs=3)",
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
barplot(i.b.7.1.t.0.81, sub="(h) with i=7.1(#ecs=7), b=7.1(#ecs=7), t=0.81(#ecs=21)",
        beside=TRUE, col=c("black","red", "green", "blue"), angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
reset <- function() {
  par(mfrow=c(1, 1), oma=rep(0, 4), mar=rep(0, 4), new=TRUE)
  plot(0:1, 0:1, type="n", xlab="", ylab="", axes=FALSE)
}

reset()
legend("top", legend=c("original", "incognito", "beta", "tclose"), fill=c("black", "red", "green", "blue"), cex=0.75, ncol=3, bty="n", 
       angle=c(0, 45, 90, 135), density=c(65,55,45, 35))
