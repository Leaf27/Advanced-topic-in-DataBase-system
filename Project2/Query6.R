library(rmr2)

## wordcount-signature
population =
  function(input, output = NULL,pattern = " "){
    
    ## wordcount-map function
    ## Important: Producing key or value of type Int may not work
    ##            That is why the map() produces ‘1’ as a string
    population.map = function(., lines) {
      keyval(
        lines[[4]],
        '1')
    }
    ## wordcount-reduce function
    ## Important: Reducer also write the output as string because Int
    ##            may not work
    population.reduce = function(country, counts ) {
      keyval(country, 
             toString(sum(as.integer(counts)))) 
    }
    
    ## wordcount-mapreduce job configuration
    mapreduce(
      input = input,
      output = output,
      map = population.map,
      reduce = population.reduce,
      combine = TRUE,
      input.format=make.input.format('csv',sep=',')
    )
  }

## Define inputs and outputs 
## The two commented lines can be used if you want to pass your own text or local file

## inputText = capture.output(license())
## inputPath = to.dfs(keyval(NULL, inputText)) 
inputPath = '/user/hadoop/input/customers.db'

## Execute (fire map-reduce job). The output is a binary HDFS file
output <- population(inputPath, pattern = ",")

## Get Results from HDFS
results <- from.dfs(output)

## Decompose the key and value columns
x <- results$key
y <- as.integer(results$val)

## Plot the values
barplot(y, main="CounrtyCode Frequency", xlab="CounrtyCode", ylab="Freq", names.arg=x)

## Sort the values and Plot them
x<-x[order(y,decreasing = TRUE)]
z <- sort(y,decreasing = TRUE)
barplot(z, main="Sorted CounrtyCode Frequency", xlab="CounrtyCode", ylab="Freq", names.arg=x, col="blue")


