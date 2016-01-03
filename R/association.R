train<-read.csv("/home/antorweep/git/incognito/data/islr/islr.dat", header = F)
train <- train[,-c(1)]
test_<-read.csv("/home/antorweep/git/incognito/data/islr/test.islr.dat", header = F)[,-c(1)]
test <- test_[,-9]


annonymized<-read.csv("/home/antorweep/git/incognito/data/tmp/islr/anonymized.dat/all.dat", header=F)
annonymized<-annonymized[,-c(7)]


colnames(train)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins", "wage")
colnames(test)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins")
colnames(annonymized)<-c("maritl", "race", "education", "jobclass", "health", "health_ins", "year", "age", "wage")

train$year <- as.factor(train$year) 
train$age <- as.factor(train$age) 
train$wage <- as.factor(train$wage) 

train<-data.frame(train)
train$wage<- lapply(train$wage, function(x){replace(x, x <=50, 50)})
as.factor(a)
min()

summary(train)
summary(annonymized)
summary(test)

library(arules)
rules <- apriori(train, appearance = list(rhs=as.factor(train$wage) )
inspect(rules)

rules.sorted <- sort(rules, by="lift")

# find redundant rules
subset.matrix <- is.subset(rules.sorted, rules.sorted)
subset.matrix[lower.tri(subset.matrix, diag=T)] <- NA
redundant <- colSums(subset.matrix, na.rm=T) >= 1
which(redundant)
# remove redundant rules
rules.pruned <- rules.sorted[!redundant]
inspect(rules.pruned)

plot(rules, method="graph", control=list(type="items"))
