    # Install Dev version of sparklyr from GitHub
    devtools::install_github("rstudio/sparklyr", force = TRUE)

    # Load libraries
    suppressMessages({
      library(sparklyr)
      library(tidyverse)
      library(DBI)
    }) 

    if(!file.exists("data/2008.csv.bz2"))
    {download.file("http://stat-computing.org/dataexpo/2009/2008.csv.bz2", "data/2008.csv.bz2")}
    if(!file.exists("data/2007.csv.bz2"))
    {download.file("http://stat-computing.org/dataexpo/2009/2007.csv.bz2", "data/2007.csv.bz2")}

    spark_install("2.1.0")

    conf <- spark_config()
    conf$`sparklyr.shell.driver-memory` <- "14G"
    sc <- spark_connect(master = "local", version = "2.1.0", config = conf)

    sql <- paste0("
    CREATE EXTERNAL TABLE hive_flights (
      Year INT, 
      Month INT, 
      DayofMonth INT, 
      DayOfWeek INT, 
      DepTime INT, 
      CRSDepTime INT, 
      ArrTime INT, 
      CRSArrTime INT, 
      UniqueCarrier STRING, 
      FlightNum INT, 
      TailNum INT, 
      ActualElapsedTime  INT,  
      CRSElapsedTime INT, 
      AirTime INT, 
      ArrDelay  INT,  
      DepDelay INT, 
      Origin STRING, 
      Dest STRING, 
      Distance INT, 
      TaxiIn  STRING, 
      TaxiOut STRING, 
      Cancelled INT, 
      CancellationCode INT, 
      Diverted INT, 
      CarrierDelay  INT, 
      WeatherDelay INT, 
      NASDelay INT, 
      SecurityDelay INT, 
      LateAircraftDelay INT)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
    LOCATION '" , file.path(getwd(), "data") ,  "'")
    DBI::dbGetQuery(sc, sql)

    ## Warning in max(n): no non-missing arguments to max; returning -Inf

Tidy Data & Model
-----------------

    tidy_flights <- tbl(sc, "hive_flights") %>%
      filter(!is.na(ArrDelay)) %>%
      select(DepDelay, ArrDelay, Distance) %>%
      sdf_register("spark_flights")

    tbl_cache(sc, "spark_flights")

    sample_flights <- tidy_flights %>%
      sample_frac(50)

    sample_count <- prettyNum(nrow(sample_flights) , big.mark = ",")
    print(sample_count)

    ## [1] "7,062,769"

### Model on 7,062,769 records

    reg_model <- sample_flights %>%
      ml_linear_regression("ArrDelay", c("DepDelay", "Distance"))

    ## * No rows dropped by 'na.omit' call

    summary(reg_model)

    ## Call: ml_linear_regression(., "ArrDelay", c("DepDelay", "Distance"))
    ## 
    ## Deviance Residuals: (approximate):
    ##     Min      1Q  Median      3Q     Max 
    ## -78.345  -7.702  -1.763   5.345 277.938 
    ## 
    ## Coefficients:
    ##                Estimate  Std. Error  t value  Pr(>|t|)    
    ## (Intercept) -7.4466e-01  8.7850e-03  -84.765 < 2.2e-16 ***
    ## DepDelay     1.0177e+00  1.4930e-04 6816.618 < 2.2e-16 ***
    ## Distance    -1.2382e-03  9.4363e-06 -131.212 < 2.2e-16 ***
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## R-Squared: 0.868
    ## Root Mean Squared Error: 14.13

    reg_predict <- sdf_predict(reg_model, tidy_flights) %>%
      mutate(diff = ArrDelay / prediction) 

    reg_predict

    ## Source:   query [1.413e+07 x 5]
    ## Database: spark connection master=local[4] app=sparklyr local=TRUE
    ## 
    ##    DepDelay ArrDelay Distance prediction       diff
    ##       <int>    <int>    <int>      <dbl>      <dbl>
    ## 1         7        1      389  5.8976420  0.1695593
    ## 2        13        8      479 11.8924484  0.6726958
    ## 3        36       34      479 35.2997027  0.9631809
    ## 4        30       26      479 29.1934624  0.8906104
    ## 5         1       -3      479 -0.3200321  9.3740593
    ## 6        10        3      479  8.8393283  0.3393923
    ## 7        56       47      647 55.4458270  0.8476743
    ## 8         9       -2      647  7.6136117 -0.2626874
    ## 9        47       44      647 46.2864666  0.9506018
    ## 10        3       -7      647  1.5073714 -4.6438455
    ## # ... with 1.413e+07 more rows

    spark_disconnect(sc)
