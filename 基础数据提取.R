library(RMySQL)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)

myserver <- options()$sqlserver
source('相关函数.R')
##收益数据(日/月)----------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)

price_data <- dbGetQuery(
  con,
  'select wind_code, trade_dt, s_adj_close as close from price_data where trade_dt > 20080101'
)

suspend_data <- dbGetQuery(
  con,
  'select wind_code, trade_dt, suspend from price_base where trade_dt > 20080101'
)

dbDisconnect(con)

price_data <- price_data %>% left_join(suspend_data %>% mutate(suspend = ifelse(suspend == 0, 0, 1)), by = c('wind_code', 'trade_dt'))

yield_data <- price_data %>% group_by(wind_code) %>% arrange(wind_code, trade_dt) %>%
  mutate(zf = backtest::cal_yield(close)) %>% select(-close)

yield_data_m <- yield_data %>% group_by(month = trade_dt %/% 100) %>%
  mutate(month_end = max(trade_dt)) %>%
  group_by(wind_code, month) %>%
  summarise(yield = prod(1+zf) - 1, suspend = ifelse(max(trade_dt) == month_end[1], suspend[n()], 1),
            trade_dt = month_end[1]) %>% select(-month)

yield_data_m <- yield_data_m %>% group_by(wind_code) %>% arrange(wind_code, trade_dt) %>% 
  mutate(yield = lead(yield, default = NA), suspend = lead(suspend, default = NA)) %>% 
  subset(trade_dt > 20090101) %>% na.omit

save(yield_data, yield_data_m, file = 'yield_data.RData')


##因子暴露(barra)--------------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
frame_data <-
  dbGetQuery(
    con,
    'select trade_dt, wind_code from factor_data_new where isst = 0 and firstday > 90'
  )

dbDisconnect(con)

##风险因子数据
mysql <- options()$sqlserver
con <-
  dbConnect(
    MySQL(),
    dbname = "news",
    host = mysql$host,
    port = myserver$port,
    username = mysql$username,
    password = mysql$password
  )

exposure_data <- dbGetQuery(con, 'select * from dy1d_exposure')
srisk_data <- dbGetQuery(con, 'select TICKER_SYMBOL as wind_code, TRADE_DATE as trade_dt, SRISK as srisk from dy1s_srisk')
float_value <- dbGetQuery(con, 'select TICKER_SYMBOL as wind_code, TRADE_DATE as trade_dt, NegMktValue as float_value from equ_factor_vs')

dbDisconnect(con)

##修正因子名及wind代码
exposure_data <- exposure_data %>% rename_all(funs(tolower)) %>%
  select(trade_dt = trade_date, wind_code = ticker_symbol,
         beta:conglomerates)

##修正行业
indus_data <- exposure_data %>% select(trade_dt, wind_code, bank:conglomerates) %>%
  reshape2::melt(id = c('trade_dt', 'wind_code')) %>% subset(value == 1) %>%
  rename(indus = variable) %>% select(-value)
exposure_data <- exposure_data %>% select(trade_dt:sizenl) %>%
  left_join(indus_data, by = c('wind_code', 'trade_dt'))

exposure_data <- exposure_data %>% left_join(srisk_data , by = c('wind_code','trade_dt')) %>%
  left_join(float_value %>% mutate(trade_dt = as.character(trade_dt)), by = c('wind_code','trade_dt')) %>%
  mutate(wind_code = to_wind_code(wind_code))

##暂时仅考虑月度分析
risk_data_m <- exposure_data %>% mutate(trade_dt = as.integer(trade_dt)) %>% 
  inner_join(frame_data, by = c('wind_code', 'trade_dt'))

save(risk_data_m, file = 'risk_data_barra.RData')

###风险矩阵----------------------
mysql <- options()$sqlserver
con <-
  dbConnect(
    MySQL(),
    dbname = "news",
    host = mysql$host,
    port = myserver$port,
    username = mysql$username,
    password = mysql$password
  )

risk_matrix <- dbGetQuery(con, 'select * from dy1s_covariance')

dbDisconnect(con)

risk_matrix <- risk_matrix %>% rename_all(funs(tolower)) %>%
  select(trade_dt = trade_date, factor:conglomerates) %>%
  mutate(trade_dt = as.integer(trade_dt)) %>%
  mutate(factor = tolower(factor))

risk_matrix_m <- risk_matrix %>% group_by(month = trade_dt %/% 100) %>%
  filter(trade_dt == max(trade_dt)) %>% ungroup %>% select(-month) %>%
  subset(factor != 'country')

risk_matrix_m$factor <- factor(risk_matrix_m$factor, levels = names(risk_matrix_m)[3:40])

save(risk_matrix_m, file = 'risk_matrix_barra.RData')

##指数权重数据--------------------------
con <- dbConnect(
  MySQL(),
  dbname = 'wsy_database',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)

index_weight <- dbGetQuery(con, 'select * from stock_weight_index')
dbDisconnect(con)

index_weight_m <- index_weight %>% group_by(month = trade_dt %/% 100) %>%
  filter(trade_dt == max(trade_dt)) %>% ungroup %>% select(-month)

save(index_weight_m, file = 'index_weight.RData')

##因子数据(月)-----------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)

factor_data <-
  dbGetQuery(
    con,
    'select factor_data_daily.* from factor_data_daily,  (select max(trade_dt) as trade_dt from factor_data_daily group by trade_dt div 100) as dt_list 
    where factor_data_daily.trade_dt = dt_list.trade_dt and factor_data_daily.isst = 0 and factor_data_daily.firstday > 90'
  )

dbDisconnect(con)

factor_data <- factor_data %>% select(-isst, -firstday) %>% subset(!is.na(indus))
factor_data <- factor_data %>% subset(trade_dt != 20181019)

load("index_weight.RData")
##全市场数据
factor_data_total <- factor_data %>% group_by(trade_dt) %>% 
  mutate_at(vars(-wind_code, -indus, -float_value, -trade_dt, -fund, -m_fund, -qfii, -safe, -divd_rate), extreme_scale_new) %>% ungroup
##沪深300数据
factor_data_hs300 <- index_weight_m %>% subset(index_code == '000300.SH') %>%
  select(trade_dt, wind_code) %>% 
  inner_join(factor_data, by = c('wind_code', 'trade_dt')) %>% group_by(trade_dt) %>% 
  mutate_at(vars(-wind_code, -indus, -float_value, -trade_dt, -fund, -m_fund, -qfii, -safe, -divd_rate), extreme_scale_new) %>% ungroup
##中证800数据
factor_data_zz800 <- index_weight_m %>% subset(index_code %in% c('000300.SH', '000905.SH')) %>%
  select(trade_dt, wind_code) %>% 
  inner_join(factor_data, by = c('wind_code', 'trade_dt')) %>% group_by(trade_dt) %>% 
  mutate_at(vars(-wind_code, -indus, -float_value, -trade_dt, -fund, -m_fund, -qfii, -safe, -divd_rate), extreme_scale_new) %>% ungroup

save(factor_data_total, factor_data_hs300, factor_data_zz800, file = 'factor_data.RData')

##因子收益(5日)------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)

trade_dt_list <-
  dbGetQuery(
    con,
    'select distinct trade_dt from factor_data_daily'
  )$trade_dt %>% sort
trade_dt_list <- trade_dt_list[trade_dt_list >= 20081231]
trade_dt_list <- trade_dt_list[1:length(trade_dt_list) %% 5 == 1]
factor_data_total_w <- dbGetQuery(
  con,
  'select * from factor_data_daily'
)
factor_data_total_w <- factor_data_total_w %>% subset(trade_dt %in% trade_dt_list)
dbDisconnect(con)

factor_data_total_w <- factor_data_total_w %>% subset(isst == 0 & firstday > 90) %>% 
  select(-isst, -firstday) %>% subset(!is.na(indus))

##全市场数据
factor_data_total_w <- factor_data_total_w %>% group_by(trade_dt) %>% 
  mutate_at(vars(-wind_code, -indus, -float_value, -trade_dt, -fund, -m_fund, -qfii, -safe, -divd_rate), extreme_scale_new)

factor_data_total_w <- factor_data_total_w %>% subset(trade_dt != 20181018)

##对应收益
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)

price_data <- dbGetQuery(
  con,
  'select wind_code, trade_dt, s_adj_close as close from price_data where trade_dt > 20080101'
)

suspend_data <- dbGetQuery(
  con,
  'select wind_code, trade_dt, suspend from price_base where trade_dt > 20080101'
)

dbDisconnect(con)

price_data <- price_data %>% left_join(suspend_data %>% mutate(suspend = ifelse(suspend == 0, 0, 1)), by = c('wind_code', 'trade_dt'))

price_data <- price_data %>% subset(trade_dt %in% trade_dt_list)

yield_data_w <- price_data %>% group_by(wind_code) %>% arrange(wind_code, trade_dt) %>%
  mutate(yield = backtest::cal_yield(close)) %>% select(-close)

yield_data_w <- yield_data_w %>% group_by(wind_code) %>% arrange(wind_code, trade_dt) %>% 
  mutate(yield = lead(yield, default = NA), suspend = lead(suspend, default = NA)) %>% 
  subset(trade_dt > 20090101) %>% na.omit

save(factor_data_total_w, yield_data_w, file = 'factor_data_w.RData')
