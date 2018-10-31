temp <- wsy_pf %>% subset(trade_dt > 20140101) %>% left_join(index_weight_m, by = c('wind_code', 'trade_dt')) %>% 
  mutate(weight = weight.x - ifelse(is.na(weight.y), 0, weight.y / 100)) %>% select(trade_dt, wind_code, weight) %>% 
  left_join(exposure_data_m, by = c('trade_dt','wind_code'))

temp %>% select(trade_dt:sizenl) %>% reshape2::melt(id = c('trade_dt','wind_code','weight')) %>% 
  group_by(trade_dt, variable) %>% summarise(expo = sum(weight * value)) %>% 
  ggplot(aes(x = ymd(trade_dt), y = expo)) + geom_bar(stat = 'identity') + facet_grid(variable~.)

temp %>% group_by(trade_dt, indus) %>% na.omit %>% summarise(expo = sum(weight)) %>% 
  ggplot(aes(x = ymd(trade_dt), y = expo)) + geom_bar(stat = 'identity') + facet_grid(indus~.)


library(RMySQL)
myserver <- options()$sqlserver
con <- dbConnect(
  MySQL(),
  dbname = 'news',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
sp_return <-
  dbGetQuery(
    con,
    'select TRADE_DATE as trade_dt, TICKER_SYMBOL as wind_code, spret from dy1d_specific_ret'
  )

dbDisconnect(con)
to_wind_code <- function(x)
{
  paste0(x, ifelse(substr(x,1,1) == '6', '.SH', '.SZ'))
}
sp_return <- sp_return %>% mutate(wind_code = to_wind_code(wind_code)) %>% group_by(month = as.integer(trade_dt) %/% 100, wind_code) %>% 
  summarise(spret = sum(spret))


load("exposure_m.RData")

lgd_weight <- readxl::read_excel('E:/Hfim 2.0/119/Receive/132(李国栋)/50指数成分.xlsx')
lgd_weight <- lgd_weight %>% transmute(wind_code = stock, weight = weight / 100, month = format(date, '%Y%m'))

lgd_weight <- lgd_weight %>% full_join(index_weight_m %>% 
                                         transmute(wind_code, month = as.character(trade_dt %/% 100), hs300 = weight / 100),
                                       by = c('month', 'wind_code')) %>% subset(month >= 201401)
lgd_weight <- lgd_weight %>% mutate(weight = ifelse(is.na(weight), 0, weight), 
                                    hs300 = ifelse(is.na(hs300), 0, hs300))

lgd_weight <- lgd_weight %>% group_by(month) %>% mutate(weight = weight / sum(weight) - hs300 / sum(hs300)) %>% select(-hs300) 

lgd_weight <- lgd_weight %>% left_join(sp_return  %>% ungroup %>% mutate(month = as.character(month)), by = c('wind_code', 'month')) %>% na.omit

lgd_weight <- lgd_weight %>% left_join(exposure_data_m %>% mutate(month = as.character(trade_dt %/% 100)), by = c('month', 'wind_code')) %>% na.omit


lgd_weight <- lgd_weight %>% select(wind_code, month, weight, beta:indus, spret)
factor_exp <- lgd_weight %>% select(-indus) %>% reshape2::melt(id = c('wind_code', 'month', 'weight')) %>% 
  group_by(month, variable) %>% summarise(value = sum(weight * value))
factor_exp %>% ggplot(aes(x = factor(month), y = value)) + geom_bar(stat = 'identity') + facet_grid(variable~.)

factor_indus <- lgd_weight %>% 
  group_by(month, indus) %>% summarise(value = sum(weight))
factor_indus %>% ggplot(aes(x = factor(month), y = value)) + geom_bar(stat = 'identity') + facet_grid(indus~.)

openxlsx::write.xlsx(list(factor_exp, factor_indus), 'expo.xlsx')


lgd_weight <- readr::read_csv('E:/Hfim 2.0/119/Receive/84(林学晨)/Lizong_stocks_Big.csv')
lgd_weight <- lgd_weight %>% transmute(wind_code = stock, trade_dt = date) %>% 
  group_by(month = as.character(trade_dt %/% 100)) %>% mutate(weight = 1/n()) %>% select(-trade_dt)

lgd_weight <- lgd_weight %>% full_join(index_weight_m %>% 
                                         transmute(wind_code, month = as.character(trade_dt %/% 100), hs300 = weight / 100),
                                       by = c('month', 'wind_code')) %>% subset(month >= 201401)
lgd_weight <- lgd_weight %>% mutate(weight = ifelse(is.na(weight), 0, weight), 
                                    hs300 = ifelse(is.na(hs300), 0, hs300))

lgd_weight <- lgd_weight %>% group_by(month) %>% mutate(weight = weight / sum(weight) - hs300 / sum(hs300)) %>% select(-hs300) 

lgd_weight <- lgd_weight %>% left_join(sp_return  %>% ungroup %>% mutate(month = as.character(month)), by = c('wind_code', 'month')) %>% na.omit

lgd_weight <- lgd_weight %>% left_join(exposure_data_m %>% mutate(month = as.character(trade_dt %/% 100)), by = c('month', 'wind_code')) %>% na.omit


lgd_weight <- lgd_weight %>% select(wind_code, month, weight, beta:indus, spret)
factor_exp <- lgd_weight %>% select(-indus) %>% reshape2::melt(id = c('wind_code', 'month', 'weight')) %>% 
  group_by(month, variable) %>% summarise(value = sum(weight * value))
factor_exp %>% ggplot(aes(x = factor(month), y = value)) + geom_bar(stat = 'identity') + facet_grid(variable~., scale = 'free')

factor_indus <- lgd_weight %>% 
  group_by(month, indus) %>% summarise(value = sum(weight))
factor_indus %>% ggplot(aes(x = factor(month), y = value)) + geom_bar(stat = 'identity') + facet_grid(indus~.)

openxlsx::write.xlsx(list(factor_exp, factor_indus), 'expo.xlsx')
