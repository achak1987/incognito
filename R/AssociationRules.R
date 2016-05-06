convertSalaryToRanges <- function(data) {
  data$salary[data$salary < 50] <- 0
  data$salary[data$salary >= 50 & data$salary < 100] <- 1
  data$salary[data$salary >= 100 & data$salary < 150] <- 2
  data$salary[data$salary >= 150 & data$salary < 200] <- 3
  data$salary[data$salary >= 200 & data$salary < 250] <- 4
  data$salary[data$salary >= 250 & data$salary < 300] <- 5
  data$salary[data$salary >= 300] <- 6

  data$salary[data$salary == "0"] <- "<50"
  data$salary[data$salary == "1"] <- "50-100"
  data$salary[data$salary == "2"] <- "100-150"
  data$salary[data$salary == "3"] <- "150-200"
  data$salary[data$salary == "4"] <- "200-250"
  data$salary[data$salary == "5"] <- "250-300"
  data$salary[data$salary == "6"] <- ">=300"
  return(data)
}
convertSalaryToRanges <- function(data) {
  data$salary[data$salary < 100] <- 0
  data$salary[data$salary >= 100 & data$salary < 200] <- 1
  data$salary[data$salary >= 200 & data$salary < 300] <- 2
  data$salary[data$salary >= 300] <- 3
  
  data$salary[data$salary == "0"] <- "<100"
  data$salary[data$salary == "1"] <- "100-200"
  data$salary[data$salary == "2"] <- "200-300"
  data$salary[data$salary == "3"] <- ">=300"
  return(data)
}

convertAgeToRanges <- function(data) {
  data$age[data$age < 25] <- 0
  data$age[data$age >= 25 & data$age < 35] <- 1
  data$age[data$age >= 35 & data$age < 45] <- 2
  data$age[data$age >= 45 & data$age < 55] <- 3
  data$age[data$age >= 55 & data$age < 65] <- 4
  data$age[data$age >= 65] <- 5
  
  data$age[data$age == "0"] <- "<25"
  data$age[data$age == "1"] <- "25-35"
  data$age[data$age == "2"] <- "35-45"
  data$age[data$age == "3"] <- "45-55"
  data$age[data$age == "4"] <- "55-65"
  data$age[data$age == "5"] <- ">=65"
  return(data)
}

getAssociationRules <- function(data, supp, conf) {
library(arules)
data<-convertSalaryToRanges(data)
data<-convertAgeToRanges(data)
uniqueSals <-paste("salary=", unique(data$salary), sep="")
data<-data.frame(lapply(data, factor))
#data<-data.frame(as.factor(data$year), as.factor(data$age), as.factor(data$education), as.factor(data$salary))
#colnames(data)<-c("year", "age", "education", "salary")

#Association Rule Mining
str(data)
rules <- apriori(data, parameter= list(supp=supp, conf=conf, target = "rules"))
#rules<-subset( rules, subset = rhs %pin% "salary=" ) 
#inspect(rules)
rules.sorted <- sort(rules, by="lift")
#inspect(rules.sorted)

#Pruning Redundant Rules
subset.matrix <- is.subset(rules.sorted, rules.sorted)
subset.matrix[lower.tri(subset.matrix, diag=T)] <- NA
redundant <- colSums(subset.matrix, na.rm=T) >= 1
which(redundant)
# remove redundant rules
rules.pruned <- rules.sorted[!redundant]
#inspect(rules.pruned)
return(rules.pruned)
}

original<-read.csv("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/o.t/data.csv", header = F)[,-1]
beta<-"0.7/ecAnn/data.csv"
incognito<-read.csv(paste("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/i/", beta, sep=""), header = F)[,-1]
beta<-read.csv(paste("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/b/", beta, sep=""), header = F)[,-1]
tcl<-"0.8/ecAnn/data.csv"
tclose<-read.csv(paste("/home/antorweep/git/SparkAnonymizationToolkit/data/islr_n/out/i/", tcl, sep=""), header = F)[,-1]

original<-data.frame(lapply(original, round))
incognito<-data.frame(lapply(incognito, round))
beta<-data.frame(lapply(beta, round))
tclose<-data.frame(lapply(tclose, round))

colnames(original)<-c( "year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins", "salary")
colnames(incognito)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins", "salary")
colnames(beta)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins", "salary")
colnames(tclose)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins", "salary")

rules<-getAssociationRules(original, 0.01, 0.9)
inspect(rules)
rules.i<-getAssociationRules(incognito, 0.01, 0.9)
inspect(rules.i)
rules.b<-getAssociationRules(beta, 0.01, 0.9)
inspect(rules.b)
rules.t<-getAssociationRules(tclose, 0.01, 0.9)
inspect(rules.t)

#Visualizing Association Rules
library("arulesViz")
plot(rules)
plot(rules, method="graph", control=list(type="items"))
plot(rules.i, method="graph", control=list(type="items"))
plot(rules.b, method="graph", control=list(type="items"))
plot(rules.t, method="graph", control=list(type="items"))
plot(rules, method="paracoord", control=list(reorder=TRUE))
