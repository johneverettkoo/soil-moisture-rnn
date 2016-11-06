#---------------------------------------------------------------------------------------------------
# First attempt at an RNN model for soil moisture
# Structure:
#   1. Packages and such
#   2. Grab data ...
#       a. soil moisture data from TX A&M
#       b. weather data from NOAA
#   3. Use the rnn package to train a model
#   4. Evaluate and think about why it failed
#---------------------------------------------------------------------------------------------------

# --- setup --- #

# import stuff
dp <- loadNamespace('dplyr')
td <- loadNamespace('tidyr')
noaa <- loadNamespace('rnoaa')
rnn <- loadNamespace('rnn')
import::from(magrittr, `%>%`, `%<>%`, extract, extract2, set_rownames)
import::from(foreach, foreach, `%do%`, `%dopar%`)
import::from(readr, read_delim, write_csv)
import::from(reshape2, dcast)
import::from(abind, abind)
library(ggplot2)

# set noaakey
# see https://cran.r-project.org/web/packages/countyweather/README.html
options('noaakey' = Sys.getenv('noaakey'))

# initialize a data frame of weather stations
# this is used to match up soil moisture locations to the nearest weather station
station.data <- noaa$ghcnd_stations()


# --- ETL --- #

# go to dir where i downloaded the data
# http://soilmoisture.tamu.edu/
setwd('~/tamus-soil-moisture/')

# extract all the data
moisture.df <- foreach(subdir = dir(), .combine = dp$bind_rows, .errorhandling = 'pass') %do% {
  setwd(subdir)
  
  # there are three files in this subdirectory
  # the actual sensor measurements
  file.with.readings <- dir() %>% 
    extract(grepl('readings', .))
  
  # one-row table about the sensor 
  file.with.location <- dir() %>% 
    extract(grepl('stations', .))
  
  # read in the sensor measurements
  temp.df <- read_delim(file.with.readings, delim = '\t') %>% 
    # add in a proper date column
    dp$mutate(date = as.Date(paste(Y, M, D, sep = '-'))) %>% 
    # transform to tidy data frame and make hadley proud
    td$gather('depth', 'theta', dp$matches('depth')) %>% 
    dp$mutate(depth = as.numeric(gsub('depth_', '', depth))) %>% 
    # soil moisture can't go below 0
    dp$mutate(theta = ifelse(theta < 0, NA, theta)) %>% 
    dp$select(id = stationID, 
              year = Y, 
              month = M, 
              day = D, 
              doy = DOY, 
              date, 
              depth, 
              theta) %>% 
    # add in longitude and latitude
    dp$full_join(dp$select(read_delim(file.with.location, delim = '\t'), 
                           longitude = Long, latitude = Lat, id = StationID), 
                 by = 'id')
  
  # locate the nearest weather station
  station.id <- temp.df %>% 
    noaa$meteo_nearby_stations(station_data = station.data) %>% 
    extract2(1) %>% 
    dp$arrange(distance) %>% 
    .$id %>% 
    extract(1)
  
  # query precip data from that station
  # API only allows querying one year at a time :(
  weather.df <- foreach(temp.year = unique(temp.df$year), .combine = dp$bind_rows) %do% {
    # extract the start and end dates
    start.date <- temp.df %>% 
      dp$filter(year == temp.year) %>% 
      .$date %>%
      min() %>% 
      as.character()
    end.date <- temp.df %>% 
      dp$filter(year == temp.year) %>% 
      .$date %>% 
      max() %>% 
      as.character()
    
    # query
    noaa$ncdc(datasetid = 'GHCND', 
              stationid = paste0('GHCND:', station.id), 
              datatypeid = 'PRCP', 
              startdate = start.date, 
              enddate = end.date, 
              limit = 1000)$data
  }
  
  # if we found weather data, join with sensor data
  if (!is.null(weather.df) && (nrow(weather.df) > 0)) {
    weather.df %<>% dp$transmute(date = as.Date(date), 
                                 precip.in = value / 10)
    
    temp.df %<>% dp$full_join(weather.df, by = 'date')
    
    soil.prop.df <- dir() %>% 
      extract(grepl('depths', .)) %>% 
      read_delim(delim = '\t') %>% 
      dp$transmute(id = StationID, 
                   depth = Depth_probe, 
                   sand = PercentSand / 100, 
                   silt = PercentSilt / 100, 
                   clay = PercentClay / 100, 
                   texture.class = SoilType, 
                   bulk.density.g_cm3 = BulkDensity)
    
    temp.df %<>% dp$full_join(soil.prop.df, 
                              by = c('id', 'depth'))
    
    setwd('..')
    
    return(temp.df)
  } else {
    setwd('..')
    return(NULL)
  }
}

# write to disk because of reasons
write_csv(moisture.df, '~/tamus-soil-moisture/soil-moisture.csv')


# --- so it turns out that the rnn package likes long data frames -_- --- #

year.id.df <- moisture.df %>%
  dp$select(id, year) %>% 
  dp$distinct()

doMC::registerDoMC(parallel::detectCores())
data.list <- foreach(i = seq_len(nrow(year.id.df))) %dopar% {
  temp.year <- year.id.df$year[i]
  temp.id <- year.id.df$id[i]
  temp.df <- dp$filter(moisture.df, year == temp.year, id == temp.id)
  if (all(is.na(temp.df$precip.in)) || all(is.na(temp.df$theta))) {
    out.list <- list(x.data = NULL, y.data = NULL)
  } else {
    if (nrow(temp.df) == 1460) {
      temp.df %<>% dp$bind_rows(data.frame(doy = rep(366, 4), depth = c(5, 25, 60, 75)))
    }
    
    # let's do this the dumb way ...
    temp.precip.in = temp.df %>% 
      dp$select(doy, precip.in) %>% 
      dp$distinct() %>% 
      dp$arrange(doy) %>%
      .$precip.in
    x.data <- temp.df %>% 
      dp$select(doy, depth, sand, silt, clay, bulk.density.g_cm3) %>% 
      td$gather(variable, value, 3:6) %>% 
      td$unite(temp, depth, variable) %>%
      dcast(temp ~ doy) %>% 
      set_rownames(.$temp) %>% 
      dp$select(-temp) %>%
      as.matrix() %>%
      rbind(temp.precip.in)
    
    y.data <- temp.df %>% 
      dp$select(doy, depth, theta) %>% 
      td$spread(doy, theta) %>% 
      magrittr::set_rownames(.$depth) %>% 
      dp$select(-depth) %>% 
      as.matrix()
    
    out.list <- list(x.data = x.data, y.data = y.data)
  }
  return(out.list)
}

# a lot of NULLs :(
keep.indices <- sapply(data.list, function(x) sum(is.na(x$x.data)) != 0) %>% which()
filtered.list <- data.list[keep.indices]

x.list <- lapply(filtered.list, function(x) x$x.data)
y.list <- lapply(filtered.list, function(x) x$y.data)

x.data <- do.call('abind', list(x.list, along = 3))
y.data <- do.call('abind', list(y.list, along = 3))

# so i was wrong about the long data frame ... 
x.data %<>% aperm(c(3, 2, 1))
y.data %<>% aperm(c(3, 2, 1))

# --- model building (finally) --- #

set.seed(123456)
train.size <- 150
train.indices <- sample(seq_len(dim(x.data)[1]), train.size)
test.indices <- seq_len(dim(x.data)[1]) %>% 
  extract(!(. %in% train.indices))

# doesn't work because of missing data :(
# i also tried modifying the package, which didn't work--just outputted NAs everywhere :( :( :(
full.model <- trainr(Y = y.data[train.indices, , ], 
                     X = x.data[train.indices, , ], 
                     learningrate = .1, 
                     hidden_dim = 8, 
                     numepochs = 10, 
                     batch_size = 1)


# --- second attempt --- #

# since that didn't work, let's try something much, MUCH simpler ...
# predict soil moisture at the top layer using precip and soil properties
# filter to years and locations with complete data -_-
complete.id.year.df <- moisture.df %>% 
  dp$group_by(id, year, depth) %>% 
  dp$summarise(x = sum(is.na(theta)), y = sum(is.na(precip.in))) %>% 
  dp$ungroup() %>% dp$filter(x + y == 0) %>% 
  dp$filter(depth == 5)
# so that's only 11 location-year pairs :(

filtered.moisture.df <- moisture.df %>% 
  dp$semi_join(complete.id.year.df, by = c('year', 'id', 'depth')) %>% 
  # cheating a bit--remove the 366th day for leap years
  dp$filter(doy < 366)

filtered.list <- foreach(i = seq_len(nrow(complete.id.year.df))) %do% {
  temp.year <- complete.id.year.df$year[i]
  temp.id <- complete.id.year.df$id[i]
  temp.df <- dp$filter(filtered.moisture.df, 
                       year == temp.year, 
                       id == temp.id) %>% 
    dp$arrange(doy)
  
  y.data <- temp.df$theta
  x.data <- dp$select(temp.df, 
                      precip.in, 
                      sand, 
                      silt, 
                      clay, 
                      bulk.density.g_cm3)
  return(list(x.data = x.data, y.data = y.data))
}
x.list <- lapply(filtered.list, function(x) x$x.data)
y.list <- lapply(filtered.list, function(x) x$y.data)

x.data <- do.call('abind', list(x.list, along = 3)) %>% 
  aperm(c(3, 1, 2))
y.data <- do.call('rbind', y.list)

# train on the first 6, test on the last 5
train.indices <- 1:6
test.indices <- 7:11

surface.model <- rnn$trainr(Y = y.data[train.indices, ], 
                            X = x.data[train.indices, , ], 
                            learningrate = .1, 
                            hidden_dim = c(8, 8), 
                            numepochs = 1000, 
                            batch_size = 1)

pred.df <- rnn$predictr(surface.model, x.data) %>% 
  data.frame() %>%
  `colnames<-`(seq_len(365)) %>% 
  dp$bind_cols(dp$select(complete.id.year.df, id, year)) %>% 
  td$gather('doy', 'theta.pred', 1:365) %>% 
  dp$mutate(doy = as.numeric(doy))

meas.mod.df <- dp$full_join(filtered.moisture.df, pred.df, 
                            by = c('id', 'year', 'doy'))

temp.meas.mod.plot <- meas.mod.df %>% 
  dp$filter(year == temp.year, id == temp.id) %>% 
  ggplot() + 
  geom_line(aes(x = date, y = theta, colour = 'meas')) + 
  geom_line(aes(x = date, y = theta.pred, colour = 'mod')) + 
  scale_colour_brewer(palette = 'Set1') + 
  labs(x = 'date', y = expression(theta), colour = NULL) + 
  theme(legend.position = 'bottom')

temp.precip.plot <- meas.mod.df %>% 
  dp$filter(year == temp.year, id == temp.id) %>% 
  ggplot() + 
  geom_line(aes(x = date, y = precip.in)) + 
  labs(x = NULL, y = 'precipitation [in]')

gridExtra::grid.arrange(temp.precip.plot, 
                        temp.meas.mod.plot, 
                        layout_matrix = rbind(1, 2, 2))
