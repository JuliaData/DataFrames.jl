df <- read.csv("demo/toy_example.csv")

lm.fit <- lm(A ~ B + C, data = df)

summary(lm.fit)
