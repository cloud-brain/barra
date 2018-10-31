library(RMySQL)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(RODBC)
myserver <- options()$sqlserver
##最新数据-------------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
##获取因子库最新日期
max_dt <- dbGetQuery(con, 'select max(trade_dt) as trade_dt from factor_data_daily')$trade_dt
##获取基础数据库最新日期
max_base <- dbGetQuery(con, 'select max(trade_dt) as trade_dt from price_base')$trade_dt
##最新数据则不用更新
stopifnot(max_dt < max_base)

##基准数据----------

# load('exposure_m.RData')

con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
frame_temp <-
  dbGetQuery(
    con,
    'select wind_code, trade_dt from price_base where trade_dt > 20080101'
  )
dbDisconnect(con)

##计算需要更新的时间点
frame_base <- frame_temp %>% subset(trade_dt > max_dt)


##常用函数---------
na_fill <- function(x)
{
  if(any(!is.na(x)))
  {
    first_flag <- which(!is.na(x))[1]
    x[first_flag:length(x)] <- zoo::na.locf(x[first_flag:length(x)])
  }
  x
}

##预期收益因子---------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
earn_sur_temp <-
  sqlQuery(
    con_rdf,
    "select S_INFO_WINDCODE as wind_code, ANN_DT as trade_dt, REPORT_PERIOD as report_pd, S_QFA_DEDUCTEDPROFIT as ded_profit, S_QFA_OPERATEINCOME as oper_income from AShareFinancialIndicator",
    stringsAsFactors = F
  )
odbcClose(con_rdf)
##剔除交易日和报告日缺失
earn_sur_temp <- earn_sur_temp %>% subset(!is.na(trade_dt) | !is.na(report_pd)) %>% unique

to_su <- function(x)
{
  (x[1] - mean(x[2:length(x)])) / sd(x[2:length(x)])
}

ded_profit_temp <- earn_sur_temp %>% select(wind_code:report_pd, ded_profit) %>% subset(!is.na(ded_profit))
oper_income_temp <- earn_sur_temp %>% select(wind_code:report_pd, oper_income) %>% subset(!is.na(oper_income) & oper_income != 0)

fun <- function(num_pd, trade_dt_list)
{
  output_ded <- data.frame()
  output_oper <- data.frame()
  for(i in trade_dt_list)
  {
    temp_ded <- ded_profit_temp %>% subset(trade_dt <= i)
    ##提取近12期数据
    temp_ded <- temp_ded %>% group_by(wind_code, report_pd) %>% 
      filter(trade_dt == max(trade_dt)) %>% group_by(wind_code) %>% 
      filter(report_pd > (max(report_pd) - 10000 * num_pd)) %>% 
      arrange(wind_code, desc(report_pd))
    
    temp_ded <- temp_ded %>% group_by(wind_code) %>% 
      arrange(wind_code, desc(report_pd)) %>% 
      summarise(su_ded_profit = to_su(ded_profit)) 
    output_ded <- rbind(output_ded, data.frame(trade_dt = i, temp_ded))
    
    temp_oper <- oper_income_temp %>% subset(trade_dt <= i)
    temp_oper <- temp_oper %>% group_by(wind_code, report_pd) %>% 
      filter(trade_dt == max(trade_dt)) %>% group_by(wind_code) %>% 
      filter(report_pd > (max(report_pd) - 10000 * num_pd)) %>% 
      arrange(wind_code, desc(report_pd))
    temp_oper <- temp_oper %>% group_by(wind_code) %>% arrange(wind_code, desc(report_pd)) %>% 
      summarise(su_oper_income = to_su(oper_income)) 
    output_oper <- rbind(output_oper, data.frame(trade_dt = i, temp_oper))
    print(i)
  }
  output_ded %>% full_join(output_oper, by = c('wind_code','trade_dt'))
}

trade_dt_list <- frame_base$trade_dt %>% unique %>% sort
earn_sur_4 <- fun(1, trade_dt_list)
earn_sur_8 <- fun(2, trade_dt_list)
earn_sur_12 <- fun(3, trade_dt_list)

earn_sur_data <- frame_base %>% subset(trade_dt %in% trade_dt_list) %>% 
  left_join(
    earn_sur_4 %>% rename(su_ded_profit_4 = su_ded_profit, su_oper_income_4 = su_oper_income),
    by = c('wind_code', 'trade_dt')
  ) %>%   
  left_join(
    earn_sur_8 %>% rename(su_ded_profit_8 = su_ded_profit, su_oper_income_8 = su_oper_income),
    by = c('wind_code', 'trade_dt')
  ) %>% 
  left_join(
    earn_sur_12 %>% rename(su_ded_profit_12 = su_ded_profit, su_oper_income_12 = su_oper_income),
    by = c('wind_code', 'trade_dt')
  ) 

earn_sur_data <- earn_sur_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% 
  mutate_at(vars(su_ded_profit_4:su_oper_income_12), na_fill)

result_data <- earn_sur_data

# earn_sur_data_m <- frame_temp_m %>% left_join(earn_sur_data, by = c('wind_code', 'trade_dt'))

##特异度----------
# load('exposure_m.RData')
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
price_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_dq_pctchange as zf, tot_shr * s_dq_close as total_value, s_dq_volume as vol from price_base where trade_dt > %d',
      max_dt - 20000
    )
  )

isst_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, isst from price_data where trade_dt > %d',
      max_dt - 20000
    )
  )

ft_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, firstday from factor_data where trade_dt > %d',
      max_dt - 20000
    )
  )

dbDisconnect(con)

con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
pb_temp <-
  sqlQuery(
    con_rdf,
    sprintf(
      "select S_INFO_WINDCODE as wind_code, TRADE_DT as trade_dt, S_VAL_PB_NEW as pb_lf from AShareEODDerivativeIndicator where TRADE_DT > 20080101",
      max_dt - 20000
    ),
    stringsAsFactors = F
  )
odbcClose(con_rdf)

library(WindR)
w.start()
market_index <- w.wsd("H20903.CSI","pct_chg","2008-01-01",today())$Data
free_rate <- w.wsd("SHIBOR1W.IR","vwap","2008-01-01",today())$Data

##数据合并
price_temp <- price_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol) 
price_total <- price_temp %>%   
  left_join(isst_temp, by = c('wind_code', 'trade_dt')) %>% 
  left_join(ft_temp, by = c('wind_code', 'trade_dt'))

price_total <- price_total %>% subset(isst == 0 & firstday > 90) %>% 
  left_join(pb_temp, by = c('wind_code', 'trade_dt')) %>% 
  group_by(wind_code) %>% arrange(wind_code, trade_dt) %>% 
  mutate(pb_lf = na_fill(pb_lf)) 

##指数合成
index_group <- price_total %>% na.omit %>% group_by(trade_dt) %>% 
  mutate(pb_type = cut(pb_lf, quantile(pb_lf, 0:3/3), labels = c('value','netural','growth'), include.lowest = T),
         total_type = cut(total_value, quantile(total_value, 0:2/2), labels = c('small','big'), include.lowest = T))

index_group <- index_group %>% group_by(trade_dt, pb_type, total_type) %>% 
  summarise(zf = sum(zf*total_value) / sum(total_value))

index_group <- index_group %>% group_by(trade_dt) %>% 
  summarise(smb = (sum(zf[total_type == 'small']) - sum(zf[total_type == 'big']))/3,
            sml = (sum(zf[pb_type == 'value']) - sum(zf[pb_type == 'growth']))/2)

##市场收益计算
market_index <- market_index %>% left_join(free_rate, by = 'DATETIME') %>% 
  mutate(trade_dt = format(DATETIME, '%Y%m%d') %>% as.integer, market_zf = PCT_CHG - VWAP / 250) %>% select(trade_dt, market_zf)

##合并结果
total_data <- price_temp %>% select(wind_code, trade_dt, zf, suspend) %>% left_join(index_group, by = 'trade_dt') %>% 
  left_join(market_index, by = 'trade_dt')


get_lm_m <- function(len, trade_dt_cal)
{
  trade_dt_list <- unique(frame_temp$trade_dt) %>% sort
  ##数据不足时返回NA
  fun <- function(x)
  {
    if(sum(x$suspend == 0) > 3)
    {
      temp <- summary(lm(zf ~ smb + sml + market_zf, data = subset(x, suspend == 0)))
      return(data.frame(rsq = temp$r.squared, sigma = sd(temp$residuals)))
    }else{
      return(data.frame(rsq = NA, sigma = NA))
    }
  }
  
  output <- data.frame()
  for(i in trade_dt_cal)
  {
    num_flag <- which(trade_dt_list == i)
    if(num_flag < len)
      next
    total_temp <- subset(total_data, 
                         between(trade_dt, trade_dt_list[num_flag-len+1], trade_dt_list[num_flag]))
    total_temp <- total_temp %>% group_by(wind_code) %>% 
      do(mod = fun(.), 
         spd_num = mean(.$suspend==0))
    
    total_temp <- total_temp %>% unnest(mod, spd_num) %>%
      transmute(
        wind_code,
        ivr = ifelse(spd_num < 0.5, NA, rsq),
        ivr_adj = ifelse(spd_num < 0.5, NA, sqrt(1 - rsq) * sigma)
      )
    output <- rbind(output, data.frame(trade_dt = i, total_temp))
    print(i)
  }
  output
}

trade_dt_cal <- frame_base$trade_dt %>% unique %>% sort

ivr_data_20 <- get_lm_m(20, trade_dt_cal)
ivr_data_20 <- ivr_data_20 %>% rename(ivr_20 = ivr, ivr_adj_20 = ivr_adj)

ivr_data_60 <- get_lm_m(60, trade_dt_cal)
ivr_data_60 <- ivr_data_60 %>% rename(ivr_60 = ivr, ivr_adj_60 = ivr_adj)

ivr_data_120 <- get_lm_m(120, trade_dt_cal)
ivr_data_120 <- ivr_data_120 %>% rename(ivr_120 = ivr, ivr_adj_120 = ivr_adj)

ivr_data <- ivr_data_20 %>% 
  left_join(ivr_data_60, by =c('wind_code', 'trade_dt')) %>% 
  left_join(ivr_data_120, by = c('wind_code', 'trade_dt'))

result_data <- result_data %>% left_join(ivr_data, by = c('wind_code', 'trade_dt')) 

# ivr_data_m <- frame_temp_m %>% left_join(ivr_data, by = c('wind_code', 'trade_dt'))

rm(price_total,pb_temp, index_group,total_data);gc(T)
##换手率及非流动性冲击--------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
illiq_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, abs(s_dq_pctchange) as zf, s_dq_amount as amount,
      s_dq_volume as vol, s_dq_volume / tot_shr  as turnover_total,
      s_dq_volume / float_shr  as turnover_float from price_base where trade_dt > %d',
      max_dt - 10000
    )
  )

dbDisconnect(con)

illiq_temp <- illiq_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol) 

hsl_adj <- function(hsl, suspend, k)
{
  hsl_n <- zoo::rollapplyr(hsl, k, fill = NA, FUN = function(x) mean(x, na.rm = T))
  suspend_n <- zoo::rollapplyr(suspend, k, fill = NA, FUN = function(x) mean(x, na.rm = T))
  ifelse(suspend_n >0.5, hsl_n, NA)
}

illiq_temp <- illiq_temp %>% mutate(illiq = zf / amount * 10000) %>% 
  mutate(illiq = ifelse(suspend == 0, illiq, NA),
         turnover_total = ifelse(suspend == 0, turnover_total, NA),
         turnover_float = ifelse(suspend == 0, turnover_float, NA)) %>%
  mutate(illiq = ifelse(is.infinite(illiq), 10, illiq)) %>% 
  mutate(suspend = ifelse(suspend == 0, 1, 0))


illiq_data <- illiq_temp %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% 
  mutate(tor_total_10 = hsl_adj(turnover_total, suspend, 10),
         tor_total_20 = hsl_adj(turnover_total, suspend, 20),
         tor_total_60 = hsl_adj(turnover_total, suspend, 60),
         tor_float_10 = hsl_adj(turnover_float, suspend, 10),
         tor_float_20 = hsl_adj(turnover_float, suspend, 20),
         tor_float_60 = hsl_adj(turnover_float, suspend, 60),
         illiq_10 = hsl_adj(illiq, suspend, 10),
         illiq_20 = hsl_adj(illiq, suspend, 20),
         illiq_60 = hsl_adj(illiq, suspend, 60)) %>% 
  select(-zf, -amount, -suspend)

result_data <- result_data %>% left_join(illiq_data, by = c('wind_code', 'trade_dt'))
rm(illiq_temp, illiq_data);gc(T)

##历史涨跌幅---------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
zf_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_adj_close as close, canbuy from price_data where trade_dt > %d',
      max_dt - 10000
    )
  )
suspend_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_dq_volume as vol from price_base where trade_dt > 20080101',
      max_dt - 10000
    )
  )

dbDisconnect(con)
suspend_temp <- suspend_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol) 

zf_temp <- left_join(zf_temp, suspend_temp, by = c('trade_dt', 'wind_code'))

zf_temp <- zf_temp %>% group_by(wind_code) %>% arrange(wind_code, trade_dt) %>% 
  mutate(zf = backtest::cal_yield(close)) %>% select(-close)

zf_adj <- function(zf, canbuy, suspend, k)
{
  zf_n <- RcppRoll::roll_prodr(zf + 1, k) - 1
  suspend <- dplyr::lag(suspend, k)
  ifelse(canbuy == 1 & suspend == 0, zf_n, NA)
}

zf_data <- zf_temp %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>%
  mutate(zf_10 = zf_adj(zf, canbuy, suspend, 10),
         zf_20 = zf_adj(zf, canbuy, suspend, 20),
         zf_60 = zf_adj(zf, canbuy, suspend, 60)) %>% 
  select(-zf, -canbuy, -suspend)

result_data <- result_data %>% left_join(zf_data, by = c('wind_code', 'trade_dt'))

rm(suspend_temp, price_temp, zf_data, zf_temp, ft_temp, isst_temp);gc(T)
# zf_data_m <- frame_temp_m %>% left_join(zf_data, by = c('wind_code', 'trade_dt'))

##盈利因子变动--------------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
earning_temp <-
  sqlQuery(
    con_rdf,
    "select S_INFO_WINDCODE as wind_code, REPORT_PERIOD as report_pd, ANN_DT as trade_dt, S_QFA_ROE_DEDUCTED as roe, S_QFA_ROA as roa, S_FA_PROFITTOGR as profit2gr, S_FA_ASSETSTURN as assetsturn, S_FA_DEBTTOASSETS as debt2asset from AShareFinancialIndicator",
    stringsAsFactors = F
  )
odbcClose(con_rdf)

##剔除报告期缺失
earning_temp <- earning_temp %>% unique %>% subset(!is.na(report_pd) & !is.na(trade_dt))

##获取每一期最新的报表
earn_del_data <- earning_temp %>% group_by(wind_code, trade_dt) %>% 
  filter(report_pd == max(report_pd))
earn_del_data <- frame_temp %>% full_join(earn_del_data, by = c('wind_code', 'trade_dt'))

##填充数据
earn_del_data <- earn_del_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% 
  mutate_at(vars(report_pd:debt2asset), funs(na_fill)) %>% subset(trade_dt > max_dt)


roe_data <- earn_del_data %>% select(wind_code, report_pd, trade_dt, roe) %>% 
  mutate(report_last = report_pd - 10000) %>% 
  left_join(earning_temp %>% 
              select(wind_code, trade_dt_n = trade_dt, 
                     report_last = report_pd, roe_n = roe) %>% na.omit,
            by = c('wind_code', 'report_last')) %>% na.omit %>% 
  group_by(wind_code, trade_dt) %>% filter(trade_dt_n == max(trade_dt_n))

roe_data <- roe_data %>% ungroup %>% mutate(roe_del = roe - roe_n) %>% 
  select(wind_code, trade_dt, roe_del)

roa_data <- earn_del_data %>% select(wind_code, report_pd, trade_dt, roa) %>% 
  mutate(report_last = report_pd - 10000) %>% 
  left_join(earning_temp %>% 
              select(wind_code, trade_dt_n = trade_dt, 
                     report_last = report_pd, roa_n = roa) %>% na.omit,
            by = c('wind_code', 'report_last')) %>% na.omit %>% 
  group_by(wind_code, trade_dt) %>% filter(trade_dt_n == max(trade_dt_n))

roa_data <- roa_data %>% ungroup %>% mutate(roa_del = roa - roa_n) %>% 
  select(wind_code, trade_dt, roa_del)

profit2gr_data <- earn_del_data %>% select(wind_code, report_pd, trade_dt, profit2gr) %>% 
  mutate(report_last = report_pd - 10000) %>% 
  left_join(earning_temp %>% 
              select(wind_code, trade_dt_n = trade_dt, 
                     report_last = report_pd, profit2gr_n = profit2gr) %>% na.omit,
            by = c('wind_code', 'report_last')) %>% na.omit %>% 
  group_by(wind_code, trade_dt) %>% filter(trade_dt_n == max(trade_dt_n))

profit2gr_data <- profit2gr_data %>% ungroup %>% mutate(profit2gr_del = profit2gr - profit2gr_n) %>% 
  select(wind_code, trade_dt, profit2gr_del)


assetsturn_data <- earn_del_data %>% select(wind_code, report_pd, trade_dt, assetsturn) %>% 
  mutate(report_last = report_pd - 10000) %>% 
  left_join(earning_temp %>% 
              select(wind_code, trade_dt_n = trade_dt, 
                     report_last = report_pd, assetsturn_n = assetsturn) %>% na.omit,
            by = c('wind_code', 'report_last')) %>% na.omit %>% 
  group_by(wind_code, trade_dt) %>% filter(trade_dt_n == max(trade_dt_n))

assetsturn_data <- assetsturn_data %>% ungroup %>% mutate(assetsturn_del = assetsturn - assetsturn_n) %>% 
  select(wind_code, trade_dt, assetsturn_del)

debt2asset_data <- earn_del_data %>% select(wind_code, report_pd, trade_dt, debt2asset) %>% 
  mutate(report_last = report_pd - 10000) %>% 
  left_join(earning_temp %>% 
              select(wind_code, trade_dt_n = trade_dt, 
                     report_last = report_pd, debt2asset_n = debt2asset) %>% na.omit,
            by = c('wind_code', 'report_last')) %>% na.omit %>% 
  group_by(wind_code, trade_dt) %>% filter(trade_dt_n == max(trade_dt_n))

debt2asset_data <- debt2asset_data %>% ungroup %>% mutate(debt2asset_del = debt2asset - debt2asset_n) %>% 
  select(wind_code, trade_dt, debt2asset_del)

earn_del_data <- frame_base %>% 
  left_join(roe_data, by = c('wind_code', 'trade_dt')) %>% 
  left_join(roa_data, by = c('wind_code', 'trade_dt')) %>% 
  left_join(profit2gr_data, by = c('wind_code', 'trade_dt')) %>% 
  left_join(assetsturn_data, by = c('wind_code', 'trade_dt')) %>% 
  left_join(debt2asset_data, by = c('wind_code', 'trade_dt'))

earn_del_data <- earn_del_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% 
  mutate_at(vars(roe_del:debt2asset_del), funs(na_fill))

result_data <- result_data %>% left_join(earn_del_data, by = c('wind_code', 'trade_dt')) 

rm(earning_temp);gc(T)
##停牌及交易日------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
isst_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, isst from price_data where trade_dt > %d',
      max_dt - 10000
    )
  )

firstday_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, firstday from factor_data where trade_dt > %d',
      max_dt - 10000
    )
  )

dbDisconnect(con)
other_data <- isst_temp %>% left_join(firstday_temp, by = c('wind_code', 'trade_dt'))

result_data <- result_data %>% left_join(other_data, by = c('wind_code', 'trade_dt'))
rm(other_data, firstday_temp, isst_temp);gc(T)
# other_data_m <- frame_temp_m %>% left_join(other_data, by = c('wind_code', 'trade_dt'))

##行业数据-------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
indus_temp <-
  sqlQuery(
    con_rdf,
    "select WIND_CODE as wind_code, CITICS_IND_CODE as indus, ENTRY_DT as entry_dt, REMOVE_DT as remove_dt from ASHAREINDUSTRIESCLASSCITICS",
    stringsAsFactors = F
  )
odbcClose(con_rdf)

indus_temp <- indus_temp %>% mutate(indus = ifelse(substr(indus, 1, 4) == 'b10m', 
                                     substr(indus, 1, 6), substr(indus, 1, 4)),
                      remove_dt = ifelse(is.na(remove_dt), 99999999, remove_dt))

trade_dt_list <- unique(frame_temp$trade_dt)
indus_data <- indus_temp %>% group_by(wind_code, entry_dt) %>%
  mutate(trade_dt = list(trade_dt_list[between(trade_dt_list, entry_dt, remove_dt)])) %>%
  ungroup %>% select(wind_code, indus, trade_dt) %>% unnest(trade_dt) %>% unique

result_data <- result_data %>% left_join(indus_data, by = c('wind_code', 'trade_dt'))
rm(indus_data);gc(T)

##增长数据------------------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
growth_temp <-
  sqlQuery(
    con_rdf,
    "select S_INFO_WINDCODE as wind_code, REPORT_PERIOD as report_pd, ANN_DT as trade_dt, S_QFA_YOYGR as qfa_yoygr, S_QFA_YOYSALES as qfa_yoysales, S_QFA_YOYOP as qfa_yoyop, S_QFA_YOYPROFIT as qfa_yoyprofit, S_QFA_YOYNETPROFIT as qfa_yoynetprofit from AShareFinancialIndicator",
    stringsAsFactors = F
  )
odbcClose(con_rdf)

growth_temp <- growth_temp %>% unique %>% subset(!is.na(report_pd) & !is.na(trade_dt))

##获取每一期最新的报表
growth_data <- growth_temp %>% group_by(wind_code, trade_dt) %>% 
  filter(report_pd == max(report_pd))
##填充缺失值
growth_data <- frame_temp %>% full_join(growth_data, by = c('wind_code', 'trade_dt'))
growth_data <- growth_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% mutate_at(vars(qfa_yoygr:qfa_yoynetprofit), funs(na_fill)) %>% select(-report_pd)


result_data <- result_data %>% left_join(growth_data, by = c('wind_code', 'trade_dt'))
gc(growth_data, growth_temp);gc(T)

##单季度收益数据----------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
qfa_earning_temp <-
  sqlQuery(
    con_rdf,
    "select S_INFO_WINDCODE as wind_code, REPORT_PERIOD as report_pd, ANN_DT as trade_dt, S_QFA_OPERATEINCOME as qfa_operateincome, S_QFA_DEDUCTEDPROFIT as qfa_dedprofit from AShareFinancialIndicator",
    stringsAsFactors = F
  )
odbcClose(con_rdf)

qfa_earning_temp <- qfa_earning_temp %>% unique %>% subset(!is.na(report_pd) & !is.na(trade_dt))

##获取每一期最新的报表
qfa_earning_data <- qfa_earning_temp %>% group_by(wind_code, trade_dt) %>% 
  filter(report_pd == max(report_pd))
##填充缺失值
qfa_earning_data <- frame_temp %>% full_join(qfa_earning_data, by = c('wind_code', 'trade_dt'))
qfa_earning_data <- qfa_earning_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% mutate_at(vars(qfa_operateincome:qfa_dedprofit), funs(na_fill)) %>% select(-report_pd)


result_data <- result_data %>% left_join(qfa_earning_data, by = c('wind_code', 'trade_dt'))
rm(qfa_earning_data);gc(T)

##真实波动率及波动率-------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
atr_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_dq_preclose as preclose, s_dq_high as high, s_dq_low as low,  s_dq_volume as vol from price_base where trade_dt > %d',
      max_dt - 10000
    )
  )
dbDisconnect(con)

atr_temp <- atr_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol) 

##停牌填空值
atr_data <- atr_temp %>% mutate(atr = pmax(abs(high - low), abs(high - preclose), abs(low - preclose)) / preclose) %>% 
  mutate(atr = ifelse(suspend != 0, NA, atr))

##当日停牌及超过半数时间停牌为空值
fun <- function(x)
{
  if(mean(is.na(x)) > 0.5 | is.na(x[length(x)]))
  {
    return(NA)
  }else{
    return(mean(x, na.rm = T))
  }
}

atr_data <- atr_data %>% 
  group_by(wind_code) %>%
  arrange(wind_code, trade_dt) %>%
  mutate(atr_20 = zoo::rollapplyr(atr, width = 20, FUN = fun, fill = NA),
         atr_60 = zoo::rollapplyr(atr, width = 60, FUN = fun, fill = NA)) %>% 
  select(wind_code, trade_dt, atr_20, atr_60) %>% ungroup

result_data <- result_data %>% left_join(atr_data, by = c('wind_code', 'trade_dt'))
# atr_data_m <- frame_temp_m %>% left_join(atr_data, by = c('wind_code', 'trade_dt'))
rm(atr_data, atr_temp);gc(T)

##估值系列-------------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
bp_temp <-
  sqlQuery(
    con_rdf,
    sprintf(
      "select S_INFO_WINDCODE as wind_code, TRADE_DT as trade_dt, 1/S_VAL_PB_NEW as bp_lf from AShareEODDerivativeIndicator where TRADE_DT > %d",
      max_dt - 20000
    ),
    stringsAsFactors = F
  )
odbcClose(con_rdf)

con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
value_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, ep_ttm, cfp_ttm, sp_ttm, cash_price_ttm from factor_data where trade_dt > %d',
      max_dt - 20000
    )
  )
dbDisconnect(con)

value_data <- value_temp %>% left_join(bp_temp, by = c('wind_code', 'trade_dt'))

result_data <- result_data %>% left_join(value_data, by = c('wind_code', 'trade_dt'))

##股息率-----------------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
divd_temp <-
  sqlQuery(
    con_rdf,
    sprintf(
      "select S_INFO_WINDCODE as wind_code, TRADE_DT as trade_dt, 1/S_PRICE_DIV_DPS as divd_rate from AShareEODDerivativeIndicator where TRADE_DT > %d",
      max_dt - 10000
    ),
    stringsAsFactors = F
  )
odbcClose(con_rdf)

divd_data <- frame_temp %>% left_join(divd_temp, by = c('wind_code', 'trade_dt'))

divd_data <- divd_data %>% mutate(divd_rate = ifelse(is.na(divd_rate), 0, divd_rate))

result_data <- result_data %>% left_join(divd_data, by = c('wind_code', 'trade_dt'))
rm(divd_data, value_data,value_temp,bp_temp, divd_temp);gc(T)

##盈利因子----------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
earning_temp <-
  sqlQuery(
    con_rdf,
    "select S_INFO_WINDCODE as wind_code, REPORT_PERIOD as report_pd, ANN_DT as trade_dt, S_QFA_ROE_DEDUCTED as roe, S_QFA_ROA as roa, S_FA_PROFITTOGR as profit2gr, S_FA_ASSETSTURN as assetsturn, S_FA_DEBTTOASSETS as debt2asset from AShareFinancialIndicator",
    stringsAsFactors = F
  )
odbcClose(con_rdf)

##剔除报告期缺失
earning_temp <- earning_temp %>% unique %>% subset(!is.na(report_pd) & !is.na(trade_dt))

##获取每一期最新的报表
earning_data <- earning_temp %>% group_by(wind_code, trade_dt) %>% 
  filter(report_pd == max(report_pd))
##填充缺失值
earning_data <- frame_base %>% full_join(earning_data, by = c('wind_code', 'trade_dt'))
earning_data <- earning_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% mutate_at(vars(roe:debt2asset), funs(na_fill)) %>% select(-report_pd)

result_data <- result_data %>% left_join(earning_data, by = c('wind_code', 'trade_dt'))


##机构持股----------
con_rdf <- odbcConnect('wdf', 'dbreader', 'dbreader')
holding_temp <-
  sqlQuery(
    con_rdf,
    "select S_INFO_WINDCODE as wind_code, REPORT_PERIOD as report_pd, ANN_DATE as ann_dt, S_HOLDER_HOLDERCATEGORY as type, S_HOLDER_PCT as holder_pct from AShareinstHolderDerData
    where S_HOLDER_HOLDERCATEGORY in ('QFII', '保险公司', '基金', '社保基金', '银行')",
    stringsAsFactors = F
  )
odbcClose(con_rdf)

holding_temp$type[holding_temp$type == 'QFII'] <- 'qfii'
holding_temp$type[holding_temp$type == '保险公司'] <- 'safe'
holding_temp$type[holding_temp$type == '基金'] <- 'fund'
holding_temp$type[holding_temp$type == '社保基金'] <- 'm_fund'
holding_temp$type[holding_temp$type == '银行'] <- 'bank'

holding_temp <- holding_temp %>% group_by(wind_code, report_pd, ann_dt, type) %>% summarise(holder_pct = sum(holder_pct, na.rm = T))
holding_temp <- holding_temp %>%
  reshape2::dcast(wind_code + report_pd + ann_dt ~ type, value.var = 'holder_pct')

##不能超过下一个公告期的最小披露日
holding_temp <- holding_temp %>% left_join(
  holding_temp %>% group_by(wind_code, report_pd) %>% summarise(min_dt = min(ann_dt)) %>%
    mutate(min_dt = lead(min_dt, default = 99999999)),
  by = c('report_pd', 'wind_code')
) %>%
  subset(ann_dt < min_dt) %>% select(-min_dt)

holding_data <- holding_temp %>% rename(trade_dt = ann_dt) %>% select(-report_pd) %>% 
  right_join(frame_temp, by = c('wind_code', 'trade_dt')) %>% 
  group_by(wind_code) %>% 
  mutate_at(vars(bank:safe), na_fill) %>% 
  mutate_at(vars(bank:safe), funs(ifelse(is.na(.), 0, .)))

##银行持股占比非0的太少，因此剔除。
holding_data <- holding_data %>% select(-bank)

result_data <- result_data %>% left_join(holding_data, by = c('wind_code', 'trade_dt'))
rm(holding_data, holding_temp);gc(T)

##市值因子相关-----------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
market_value_data <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_dq_close * tot_shr as total_value, s_dq_close * float_shr as float_value from price_base where trade_dt > %d',
      max_dt
    )
  )
dbDisconnect(con)

market_value_data <- frame_base %>% left_join(market_value_data, by = c('wind_code', 'trade_dt'))
market_value_data <- market_value_data %>% 
  mutate(float_value_ln = log(float_value), total_value_ln = log(total_value),
         float_value_ln2 = float_value_ln^2, total_value_ln2 = total_value_ln^2,
         float_value_ln3 = float_value_ln^3, total_value_ln3 = total_value_ln^3)

result_data <- result_data %>% left_join(market_value_data, by = c('wind_code', 'trade_dt'))

##beta----------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
beta_data_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_dq_pctchange as zf, s_dq_volume as vol from price_base where trade_dt > %d',
      max_dt - 20000
    )
  )
dbDisconnect(con)

beta_data_temp <- beta_data_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol)

library(WindR)
w.start()
free_rate <- w.wsd("SHIBOR1W.IR","vwap","2008-01-01",today())$Data
hs300 <- w.wsd("000300.SH","pct_chg","2008-01-01",today())$Data
zz500 <- w.wsd("000905.SH","pct_chg","2008-01-01",today())$Data
zzqz <- w.wsd("H20903.CSI","pct_chg","2008-01-01",today())$Data

beta_data_temp <- beta_data_temp %>% left_join(
  free_rate %>% 
    transmute(trade_dt = format(DATETIME, '%Y%m%d') %>% as.integer,
              free_rate = VWAP / 250) %>%
    left_join(hs300 %>% transmute(
      trade_dt = format(DATETIME, '%Y%m%d') %>% as.integer,
      hs300 = PCT_CHG), by = c('trade_dt')) %>%
    left_join(zz500 %>% transmute(
      trade_dt = format(DATETIME, '%Y%m%d') %>% as.integer,
      zz500 = PCT_CHG), by = c('trade_dt')) %>%
    left_join(zzqz %>% transmute(
      trade_dt = format(DATETIME, '%Y%m%d') %>% as.integer,
      zzqz = PCT_CHG), by = c('trade_dt')), by = 'trade_dt')



get_lm_m <- function(len, trade_dt_cal)
{
  trade_dt_list <- unique(frame_temp$trade_dt) %>% sort
  ##数据不足时返回NA
  fun <- function(x)
  {
    if(mean(sum(x$suspend == 0) > 4))
    {
      return(data.frame(beta_hs300 = 
                          lm(zf ~ index, data = subset(x, suspend == 0) %>% 
                               mutate(zf = zf - free_rate, index = hs300 - free_rate))$coef['index'],
                        beta_zz500 = 
                          lm(zf ~ index, data = subset(x, suspend == 0) %>% 
                               mutate(zf = zf - free_rate, index = zz500 - free_rate))$coef['index'],
                        beta_zzqz = 
                          lm(zf ~ index, data = subset(x, suspend == 0) %>% 
                               mutate(zf = zf - free_rate, index = zzqz - free_rate))$coef['index']))
      
    }else{
      return(data.frame(beta_hs300 = NA, beta_zz500 = NA, beta_zzqz = NA))
    }
  }
  output <- data.frame()
  for(i in trade_dt_cal)
  {
    num_flag <- which(trade_dt_list == i)
    if(num_flag < len)
      next
    total_temp <- subset(beta_data_temp, 
                         between(trade_dt, trade_dt_list[num_flag-len+1], trade_dt_list[num_flag]))
    total_temp <- total_temp %>% group_by(wind_code) %>% 
      do(mod = fun(.), 
         spd_num = mean(.$suspend==0))
    total_temp <- total_temp %>% unnest(mod, spd_num) %>%
      transmute(
        wind_code,
        beta_hs300 = ifelse(spd_num < 0.5, NA, beta_hs300),
        beta_zz500 = ifelse(spd_num < 0.5, NA, beta_zz500),
        beta_zzqz = ifelse(spd_num < 0.5, NA, beta_zzqz)
      )
    output <- rbind(output, data.frame(trade_dt = i, total_temp))
    print(i)
  }
  output
}

trade_dt_cal <- frame_base$trade_dt %>% unique %>% sort
beta_20 <- get_lm_m(20, trade_dt_cal)
beta_60 <- get_lm_m(60, trade_dt_cal)
beta_120 <- get_lm_m(120, trade_dt_cal)

beta_data <- beta_20 %>% rename_at(vars(beta_hs300:beta_zzqz), function(x) paste0(x,'_20')) %>% 
  left_join(beta_60 %>% rename_at(vars(beta_hs300:beta_zzqz), function(x) paste0(x,'_60')), by = c('trade_dt', 'wind_code')) %>% 
  left_join(beta_120 %>% rename_at(vars(beta_hs300:beta_zzqz), function(x) paste0(x,'_120')), by = c('trade_dt', 'wind_code'))

result_data <- result_data %>% left_join(beta_data, by = c('wind_code', 'trade_dt'))

rm(beta_data);gc(T)
##成交金额-------------------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
amount_data <- dbGetQuery(
  con,
  sprintf(
    'select wind_code, trade_dt, s_dq_amount as amount, s_dq_volume as vol from price_base where trade_dt > %d',
    max_dt - 10000
  )
)

dbDisconnect(con)

amount_data <- amount_data %>% mutate(amount = ifelse(vol == 0, NA, amount))

fun <- function(x, k)
{
  if(mean(is.na(x)) > 0.5)
  {
    return(NA)
  }else{
    return(mean(x, na.rm = T))
  }
}

amount_data <- amount_data %>% 
  group_by(wind_code) %>% arrange(wind_code, trade_dt) %>% 
  mutate(amount_20_ln = zoo::rollapplyr(amount, width = 20, FUN = fun, fill = NA),
         amount_60_ln = zoo::rollapplyr(amount, width = 60, FUN = fun, fill = NA),
         amount_120_ln = zoo::rollapplyr(amount, width = 120, FUN = fun, fill = NA)) %>% 
  select(-amount, -vol)

result_data <- result_data %>% left_join(amount_data, by = c('wind_code', 'trade_dt'))

##收益标准差------------------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
zf_sd_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_adj_close as close from price_data where trade_dt > %d',
      max_dt - 10000
    )
  )
suspend_temp <-
  dbGetQuery(
    con,
    sprintf(
      'select wind_code, trade_dt, s_dq_volume as vol from price_base where trade_dt > 20080101',
      max_dt - 10000
    )
  )

dbDisconnect(con)
suspend_temp <- suspend_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol) 

zf_sd_data <- zf_sd_temp %>% group_by(wind_code) %>% arrange(wind_code, trade_dt) %>% 
  mutate(zf = backtest::cal_yield(close)) %>% select(-close)

zf_sd_data <- zf_sd_data %>% left_join(suspend_temp, by = c('trade_dt', 'wind_code')) %>% 
  mutate(zf = ifelse(suspend == 0, zf, NA))

fun <- function(zf, suspend, k)
{
  zf_sd_n <- RcppRoll::roll_sdr(zf, k, na.rm = T)
  suspend <- RcppRoll::roll_meanr(suspend, k, na.rm = T)
  ifelse(suspend < 0.5, zf_sd_n, NA)
}

zf_sd_data <- zf_sd_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>%
  mutate(zf_sd_20 = fun(zf, suspend, 20),
         zf_sd_60 = fun(zf, suspend, 60),
         zf_sd_120 = fun(zf, suspend, 120)) %>% 
  select(-zf, -suspend)

result_data <- result_data %>% left_join(zf_sd_data, by = c('wind_code', 'trade_dt'))
rm(suspend_temp, beta_data, amount_data, zf_sd_data, beta_data_temp);gc(T)

##价格及趋势---------------
con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
price_temp <-
  dbGetQuery(con,
             sprintf(
             'select wind_code, trade_dt, s_adj_close as price from price_data where trade_dt > %d',
             max_dt - 20000))
suspend_temp <-
  dbGetQuery(con,
             sprintf(
               'select wind_code, trade_dt, s_dq_volume as vol, s_dq_close as close_price from price_base where trade_dt > %d',
               max_dt - 20000))

dbDisconnect(con)
suspend_temp <- suspend_temp %>% mutate(suspend = ifelse(vol != 0, 0, 1)) %>% select(-vol) 


price_data <- price_temp %>% left_join(suspend_temp, by = c('trade_dt', 'wind_code')) %>% 
  mutate(close_price = ifelse(suspend == 0, close_price, NA),
         price = ifelse(suspend == 0, price, NA))

fun <- function(price, suspend, m, n)
{
  price_m <- RcppRoll::roll_meanr(price, m, na.rm = T)
  suspend_m <- RcppRoll::roll_meanr(suspend, m, na.rm = T)
  price_n <- RcppRoll::roll_meanr(price, n, na.rm = T)
  suspend_n <- RcppRoll::roll_meanr(suspend, n, na.rm = T)
  ifelse(suspend_m < 0.5 & suspend_n < 0.5, price_m / price_n - 1, NA)
}

price_data <- price_data %>% group_by(wind_code) %>% 
  arrange(wind_code, trade_dt) %>% 
  mutate(trend_5_60 = fun(price, suspend, 5, 60),
         trend_5_120 = fun(price, suspend, 5, 120),
         trend_5_240 = fun(price, suspend, 5, 240)) %>% select(-price, -suspend)

result_data <- result_data %>% left_join(price_data, by = c('wind_code', 'trade_dt'))
# price_data_m <- frame_temp_m %>% left_join(price_data, by = c('wind_code', 'trade_dt'))



##数据汇总-------------

con <- dbConnect(
  MySQL(),
  dbname = 'quant',
  username = myserver$username,
  password = myserver$password,
  host = myserver$host,
  port = myserver$port
)
dbWriteTable(
  con,
  "factor_data_daily",
  result_data %>% data.frame,
  append = T,
  row.names = F,
  field.types = list(
    wind_code = 'char(9)',
    trade_dt = 'int(8)',
    indus = 'char(6)',
    firstday = 'int(5)',
    isst = 'tinyint(1)',
    roe_del = 'double',
    roa_del = 'double',
    profit2gr_del = 'double',
    assetsturn_del = 'double',
    debt2asset_del = 'double',
    turnover_total = 'double',
    turnover_float = 'double',
    illiq = 'double',
    tor_total_10 = 'double',
    tor_total_20 = 'double',
    tor_total_60 = 'double',
    tor_float_10 = 'double',
    tor_float_20 = 'double',
    tor_float_60 = 'double',
    illiq_10 = 'double',
    illiq_20 = 'double',
    illiq_60 = 'double',
    ivr_20 = 'double',
    ivr_60 = 'double',
    ivr_120 = 'double',
    ivr_adj_20 = 'double',
    ivr_adj_60 = 'double',
    ivr_adj_120 = 'double',
    zf_10 = 'double',
    zf_20 = 'double',
    zf_60 = 'double',
    qfa_yoygr = 'double',
    qfa_yoysales = 'double',
    qfa_yoyop = 'double',
    qfa_yoyprofit = 'double',
    qfa_yoynetprofit = 'double',
    qfa_operateincome = 'double',
    qfa_dedprofit = 'double',
    atr_20 = 'double',
    atr_60 = 'double',
    divd_rate = 'double',
    roe = 'double',
    roa = 'double',
    profit2gr = 'double',
    assetsturn = 'double',
    debt2asset = 'double',
    ep_ttm = 'double',
    cfp_ttm = 'double',
    sp_ttm = 'double',
    cash_price_ttm = 'double',
    bp_lf = 'double',
    total_value = 'double',
    float_value = 'double',
    float_value_ln = 'double',
    total_value_ln = 'double',
    float_value_ln2 = 'double',
    total_value_ln2 = 'double',
    float_value_ln3 = 'double',
    total_value_ln3 = 'double',
    zf_sd_20 = 'double',
    zf_sd_60 = 'double',
    zf_sd_120 = 'double',
    close_price = 'decimal(6,2)',
    trend_5_60 = 'double',
    trend_5_120 = 'double',
    trend_5_240 = 'double',
    beta_hs300_20 = 'double',
    beta_zz500_20 = 'double',
    beta_zzqz_20 = 'double',
    beta_hs300_60 = 'double',
    beta_zz500_60 = 'double',
    beta_zzqz_60 = 'double',
    beta_hs300_120 = 'double',
    beta_zz500_120 = 'double',
    beta_zzqz_120 = 'double',
    su_ded_profit_4 = 'double',
    su_oper_income_4 = 'double',
    su_ded_profit_8 = 'double',
    su_oper_income_8 = 'double',
    su_ded_profit_12 = 'double',
    su_oper_income_12 = 'double',
    amount_20_ln = 'double',
    amount_60_ln = 'double',
    amount_120_ln = 'double',
    fund = 'double',
    m_fund = 'double',
    qfii = 'double',
    safe = 'double'
  )
)
dbDisconnect(con)


