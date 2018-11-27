library(tidyverse)
library(lubridate)
library(backtest)
library(multidplyr)

myserver <- options()$sqlserver
source('相关函数.R')

##数据获取-----------
load('index_weight.RData')
load('yield_data.RData')

library(WindR)
w.start()
hs300 <- w.wsd("000300.SH","close","2009-01-01","2018-10-31")$Data
hs300 <- hs300 %>% arrange(DATETIME) %>% transmute(trade_dt = ymd(DATETIME), close = CLOSE,
                                                   hs300 = cal_yield(close))
##相关函数---------
##每期权重计算收益
##外生变量
##@yield_data
to_yield <- function(trade_dt, wind_code, weight)
{
  ##获取每个月份对应的时间
  trade_date <- unique(yield_data$trade_dt)
  trade_frame <- data.frame(begin_date = unique(trade_dt)) %>%
    arrange(begin_date) %>% mutate(end_date = lead(begin_date, 1, default = 99999999))
  trade_frame <- trade_frame %>% group_by(begin_date) %>%
    mutate(trade_dt = list(trade_date[trade_date > begin_date &
                                        trade_date <= end_date]))
  trade_frame <-
    trade_frame %>% select(-end_date) %>% unnest(trade_dt)
  
  ##构建股票对应的权重
  temp <- data.frame(begin_date = trade_dt, wind_code, weight) %>% 
    right_join(trade_frame , by = 'begin_date')
  
  ##添加涨跌幅
  temp <- temp %>% left_join(yield_data, by = c('trade_dt', 'wind_code'))
  ##对于缺失的退市股票认为按月现金退出
  temp <- temp %>% mutate(zf = ifelse(is.na(zf), 0, zf))
  
  ##修正权重
  temp <- temp %>% group_by(trade_dt) %>% mutate(weight = weight /sum(weight)) 
  ##按区间计算涨幅
  temp <- temp %>% 
    group_by(begin_date, wind_code) %>% arrange(begin_date, wind_code, trade_dt) %>% 
    mutate(zf = cumprod(1 + zf)) %>% 
    group_by(trade_dt, begin_date) %>% summarise(zf = sum(weight * zf)) 
  ##按区间计算收益率
  temp <- temp %>% 
    group_by(begin_date) %>% mutate(zf = backtest::cal_yield(zf, first = zf[1] - 1)) %>% 
    ungroup 
  
  temp
}

to_perform <- function(trade_dt, yield, group_year = F)
{
  temp <- data.frame(trade_dt, yield)
  temp <- left_join(temp %>% transmute(trade_dt = ymd(trade_dt), yield = yield), 
                    hs300 %>% select(trade_dt, hs300), by = 'trade_dt') 
  # print(temp %>% 
  #         mutate(yield_rel = cumprod(1 + yield - hs300)) %>% 
  #         ggplot(aes(x = trade_dt, y = yield_rel)) + geom_line() + coord_trans(y = 'log10'))
  # 
  if(group_year)
  {
    return(temp %>% group_by(year = format(trade_dt, '%Y')) %>% 
             mutate(yield_rel = yield - hs300) %>% 
             summarise(y_m = mean(yield_rel) * 250, 
                       y_sd = sd(yield_rel) * sqrt(250),
                       y_sp = y_m / y_sd))
  }else{
    return(temp %>% mutate(yield_rel = yield - hs300) %>% 
             summarise(y_m = mean(yield_rel) * 250, 
                       y_sd = sd(yield_rel) * sqrt(250),
                       y_sp = y_m / y_sd))
  }
  
}

##将rp转化为权重
#@param x,tibble, 应包含预期收益value, 权重weight
#@param risk_name, 风险因子的名称
#@param bias, in_bench 偏离，基准内
to_weight <- function(x, risk_name, bias, in_bench)
{
  ##转化行业数据为矩阵并剔除一个行业
  risk_name <- setdiff(risk_name, 'indus')
  factor_total <- data.frame(x %>% select(-indus), 
                             model.matrix( ~ indus + 0, data = x))
  
  ##计算权重
  with(factor_total,
       tibble(
         wind_code = wind_code,
         weight = tf_pfp_bench(
           r_exp = value,
           x_beta = as.matrix(factor_total %>% select(one_of(risk_name), starts_with('indus'))),
           benchmark = weight, 
           bias = bias,
           in_bench = in_bench
         )
       )) %>% subset(weight != 0)
}

##将rp转化为组合收益
tf_rp2yield <- function(x, risk_data, index_data, risk_name, bias, in_bench, weight_save = F, min_trade_dt = min(x$trade_dt))
{
  temp <- to_weight(x, risk_data, index_data, risk_name, bias, in_bench) %>% subset(trade_dt >= min_trade_dt)
  ##计算净值
  temp_y <- temp %>% with(., to_yield(trade_dt, wind_code, weight)) %>% subset(trade_dt >= min_trade_dt)
  
  if(weight_save)
  {
    return(list(weight = temp %>% subset(weight != 0), yield = temp_y))
  }else{
    return(list(yield = temp_y))
  }
}

##组合求解---------------
load('rp_ir.RData')
load('factor_name.RData')

fun_nest_p <- function(rp_data, risk_name, index_weight, bias, in_bench, risk_len, cl_len = 2, weight_save = T)
{
  ##剔除停牌股票
  rp_data <- rp_data %>% left_join(yield_data %>% ungroup  %>% subset(suspend == 0) %>% select(wind_code, trade_dt) %>% nest(wind_code), by = 'trade_dt') %>% 
    mutate(value = map2(value, data, function(x, y) inner_join(x, y, by = 'wind_code'))) %>% select(-data)
  
  ##合并权重
  rp_data <-
    rp_data %>% left_join(index_weight %>% nest(-trade_dt), by = 'trade_dt') %>%
    mutate(value = map2(value, data, function(x, y)
      left_join(x, y, by = 'wind_code') %>% mutate(weight = ifelse(is.na(weight), 0, weight)) %>%
        mutate(weight = weight / sum(weight)))) %>% select(-data)
  
  ##调整因子名称与风险因子日期一致
  risk_name <- risk_name %>% group_by(begin_dt, end_dt) %>% 
    mutate(trade_dt = list(tibble(trade_dt = rp_data$trade_dt) %>% subset(trade_dt <= end_dt & trade_dt > begin_dt))) %>% 
    ungroup %>% select(trade_dt, end_dt, factor_name) %>% 
    unnest(trade_dt, .drop = F) %>% 
    group_by(trade_dt) %>% filter(end_dt == min(end_dt)) %>% select(-end_dt)
  
  ##合并风险因子名称
  rp_data <- rp_data %>% left_join(risk_name, by = 'trade_dt')
  
  fun <- function(bias, in_bench, risk_len)
  {
    temp_w <- rp_data %>%
      transmute(trade_dt,
                weight = map2(value, factor_name, function(x, y)
                  to_weight(
                    x,
                    risk_name = y$risk_name[1:risk_len],
                    bias = bias,
                    in_bench = in_bench
                  ))) %>% unnest
    
    ##计算净值
    temp_y <- temp_w %>% with(., to_yield(trade_dt, wind_code, weight))
    list(weight = temp_w, yield = temp_y)
  }
    
  
  param <- expand.grid(bias = bias, in_bench = in_bench, risk_len = risk_len)
  if(cl_len > 1)
  {
    cl <- create_cluster(cl_len) %>% cluster_library(c('tidyverse')) %>% 
      cluster_copy(fun) %>% 
      cluster_copy(rp_data) %>% 
      cluster_copy(index_weight) %>% 
      cluster_copy(tf_pfp_bench) %>% 
      cluster_copy(to_yield) %>%
      cluster_copy(to_weight) %>%
      cluster_copy(yield_data) %>% 
      cluster_copy(weight_save)
    
    temp_total <- param %>% partition(bias, in_bench, risk_len, cluster = cl) %>% 
      mutate(result = list(fun(
        bias = bias,
        in_bench = in_bench,
        risk_len = risk_len
      ))) %>% collect()
  }else{
    temp_total <- param %>% group_by(bias, in_bench, risk_len) %>%
      mutate(result = list(fun(
        bias = bias,
        in_bench = in_bench,
        risk_len = risk_len
      )))
  }
  
  temp_total %>% mutate(result = map(result, function(x)
    c(x,
      list(
        perform = with(x$yield %>% arrange(trade_dt),
                       to_perform(trade_dt, zf))
      ))))
}

total_sq <- fun_nest_p(
  rp_data_sq,
  factor_name$factor_sq,
  index_weight = index_weight %>% subset(index_code == '000300.SH') %>% select(-index_code),
  bias = 1:10 / 100,
  in_bench = seq(0.7, 0.95, by = 0.05),
  risk_len = 2:10,
  cl_len = 4
)

total_eq <- fun_nest_p(
  rp_data_eq,
  factor_name$factor_eq,
  index_weight = index_weight %>% subset(index_code == '000300.SH') %>% select(-index_code),
  bias = 1:10 / 100,
  in_bench = seq(0.7, 0.95, by = 0.05),
  risk_len = 2:10,
  cl_len = 4
)

total_sq_w <- fun_nest_p(
  rp_data_sq_w,
  factor_name$factor_sq_w,
  index_weight = index_weight %>% subset(index_code == '000300.SH') %>% select(-index_code),
  bias = 1:10 / 100,
  in_bench = seq(0.7, 0.95, by = 0.05),
  risk_len = 2:10,
  cl_len = 4
)

total_sq_3y <- fun_nest_p(
  rp_data_sq_3y,
  factor_name$factor_sq_3y,
  index_weight = index_weight %>% subset(index_code == '000300.SH') %>% select(-index_code),
  bias = 1:10 / 100,
  in_bench = seq(0.7, 0.95, by = 0.05),
  risk_len = 2:10,
  cl_len = 4
)

save(total_sq, total_eq, total_sq_w, total_sq_3y, total_sq_5y, file = 'result_weight.RData')
