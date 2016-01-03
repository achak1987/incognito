set.seed(500)
train<-read.csv("/home/antorweep/git/incognito/data/islr/islr.dat", header = F)
train <- train[,-c(1)]
test_<-read.csv("/home/antorweep/git/incognito/data/islr/test.islr.dat", header = F)[,-c(1)]
test <- test_[,-9]
annonymized<-read.csv("/home/antorweep/git/incognito/data/tmp/islr/anonymized.dat/all.dat", header=F)
annonymized<-annonymized[,-c(7)]


colnames(train)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins", "wage")
colnames(test)<-c("year", "age", "maritl", "race", "education", "jobclass", "health", "health_ins")
colnames(annonymized)<-c("maritl", "race", "education", "jobclass", "health", "health_ins", "year", "age", "wage")

levels(annonymized$race) <- levels(train$race)
levels(annonymized$maritl)<- levels(train$maritl)

summary(train)
summary(annonymized)
summary(test)

library("neuralnet")
modFit<-neuralnet(wage ~ ., method = "lm",data=train, hidden = 4, lifesign = "minimal", 
                  linear.output = FALSE, threshold = 0.1
modFitAnnonymized<-train(wage ~ ., method = "lm",data=annonymized)

wagePred <- compute(modFit, test)
wagePredAnnonymized <- compute(modFitAnnonymized, test)

mape<-sum(abs(test_$V10 - wagePred) / test_$V10) / nrow(test)
mapeAnonymized<-sum(abs(test_$V10 - wagePredAnnonymized) / test_$V10) / nrow(test)

