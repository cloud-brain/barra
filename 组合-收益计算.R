##用于详细收益计算
library(tidyverse)
library(backtest)
library(RMySQL)

##数据---------------
load('result_data.RData')
load('yield_data.RData')
total_sq_ch <- total_sq_ch %>% mutate(result = map(result, ~.$weight)) %>% unnest(result) %>% subset(weight != 0)

total_sq_ch <- total_sq_ch %>% left_join(yield_data %>% select(-zf), by = c('wind_code', 'trade_dt')) %>% 
  subset(suspend == 0)

total_sq_m <- total_sq_ch %>% ungroup %>% subset(bias == 0.03 & in_bench == 0.95 & risk_len == 7) %>% 
  select(trade_dt, wind_code, weight) %>% 
  subset(weight > 0.00001) %>% 
  group_by(trade_dt) %>% 
  mutate(weight = weight / sum(weight))

total_sq_sp <- total_sq_ch %>% ungroup %>% subset(bias == 0.01 & in_bench == 0.8 & risk_len == 9) %>% 
  select(trade_dt, wind_code, weight) %>% 
  subset(weight > 0.00001) %>% 
  group_by(trade_dt) %>% 
  mutate(weight = weight / sum(weight))

##收益计算---------
myserver <- options()$sqlserver
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
) %>% as.rdf

fun <- function(con, total_data)
{
  total_data <- total_data %>% ungroup %>% 
    mutate(buy_date = next_buz_day(con, trade_dt) %>% ymd)
  
  hs300_acount <- stock_acount$new(con = con)
  td_list <- c(unique(total_data$buy_date), ymd(20181031))
  for(i in seq_len(length(td_list) - 1))
  {
    temp <- subset(total_data, buy_date == td_list[i])
    hs300_acount$order_to(td_list[i], temp$wind_code, temp$weight)
    temp_dt <- get_buz_day(con, td_list[i], td_list[i+1])
    temp_dt <- temp_dt[-length(temp_dt)]
    hs300_acount$acount_update(ymd(temp_dt))
  }
  return(hs300_acount)
}

acount_sq_m <- fun(con, total_sq_m)
acount_sq_sp <- fun(con, total_sq_sp)
    
dbDisconnect(con$con)


##收益对比------------
library(WindR)
w.start()
hs300 <- w.wsd("000300.SH","close","2009-01-01","2018-10-31")$Data
hs300 <- hs300 %>% arrange(DATETIME) %>% transmute(date = ymd(DATETIME), hs300 = CLOSE)

total_acount <- acount_sq_m$show_total_acount() %>% 
  left_join(acount_sq_sp$show_total_acount() %>% 
              rename(benchmark = acount), by = c('date'))

total_acount <- acount_sq_sp$show_total_acount() %>% 
  left_join(hs300, by = c('date')) %>% 
  mutate(benchmark = hs300/hs300[1] *acount[1]) %>% select(-hs300)

summary_acount(total_acount, benchmark = T)
