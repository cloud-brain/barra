library(RMySQL)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(backtest)

myserver <- options()$sqlserver
source('相关函数.R')

##数据获取-----------
load('index_weight.RData')
load('factor_name.RData')
load('yield_data.RData')

library(WindR)
w.start()
hs300 <- w.wsd("000300.SH","close","2009-01-01","2018-10-25")$Data
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

##将rp转化为组合收益
tf_rp2yield <- function(x, risk_data, index_data, risk_name, bias, in_bench)
{
  ##合并权重数据
  factor_total <- x %>% left_join(index_data, by = c('trade_dt', 'wind_code')) %>% 
    mutate(weight = ifelse(is.na(weight), 0, weight / 100)) %>% 
    group_by(trade_dt) %>% mutate(weight = weight / sum(weight))
  
  ##合并风险因子
  factor_total <- factor_total %>% left_join(risk_data, by = c('trade_dt', 'wind_code')) %>% na.omit
  
  ##转化行业数据为矩阵并剔除一个行业
  factor_total <- data.frame(factor_total %>% select(-indus), 
                             model.matrix( ~ indus + 0, data = factor_total))
  
  ##计算权重
  temp <- factor_total %>% group_by(trade_dt) %>%
    do(weight = data.frame(
      wind_code = .$wind_code,
      weight = tf_pfp_bench(
        r_exp = .$value,
        x_beta = as.matrix(select(., one_of(risk_name), starts_with('indus'))),
        benchmark = .$weight, 
        bias = bias,
        in_bench = in_bench
      )
    ))
  temp <- temp %>% unnest(weight)
  
  ##计算净值
  temp_y <- temp %>% with(., to_yield(trade_dt, wind_code, weight))
}

##组合求解---------------
load('rp_ir.RData')

fun <- function(rp_data, factor_temp, risk_name, bias, in_bench, risk_len)
{
  param <- expand.grid(bias = bias, in_bench = in_bench, risk_len = risk_len)
  temp_total <- mapply(function(bias, in_bench, risk_len) 
    data.frame(
      bias = bias,
      in_bench = in_bench,
      risk_len = risk_len,
      tf_rp2yield(
        rp_data,
        factor_temp,
        index_data = index_weight_m %>% subset(index_code == '000300.SH') %>% select(-index_code),
        risk_name = risk_name[1:risk_len],
        bias,
        in_bench
      )
    ), bias = param$bias, in_bench = param$in_bench, risk_len = param$risk_len, SIMPLIFY = F) %>% 
    do.call('rbind', .) %>% 
    subset(trade_dt >= 20100101)
  
  list(yield = temp_total,
       result = temp_total %>% 
         group_by(bias, in_bench, risk_len) %>% arrange(trade_dt) %>%
         do(result = to_perform(.$trade_dt, .$zf)) %>% unnest(result))
}

fun_p <- function(rp_data, factor_temp, risk_name, bias, in_bench, risk_len, cl_len = 2)
{
  cl <- create_cluster(cl_len) %>% cluster_library(c('dplyr','tidyr')) %>% 
    cluster_copy(tf_rp2yield) %>% 
    cluster_copy(rp_data) %>% 
    cluster_copy(factor_temp) %>%
    cluster_copy(index_weight_m) %>% 
    cluster_copy(risk_name) %>% 
    cluster_copy(tf_pfp_bench) %>% 
    cluster_copy(to_yield) %>%
    cluster_copy(yield_data)
  param <- expand.grid(bias = bias, in_bench = in_bench, risk_len = risk_len)
  temp_total <- param %>% partition(bias, in_bench, risk_len, cluster = cl) %>% 
    mutate(result = list(tf_rp2yield(
      rp_data,
      factor_temp,
      index_data = index_weight_m %>% subset(index_code == '000300.SH') %>% select(-index_code),
      risk_name = risk_name[1:risk_len],
      bias,
      in_bench
    ))) %>% collect() %>% unnest(result) %>% 
    subset(trade_dt >= 20100101)
  
  list(yield = temp_total,
       result = temp_total %>% 
         group_by(bias, in_bench, risk_len) %>% arrange(trade_dt) %>%
         do(result = to_perform(.$trade_dt, .$zf)) %>% unnest(result))
}

total_sq <- fun_p(
  rp_data_sq,
  factor_temp_sq,
  factor_name$factor_sq$risk_name,
  bias = 1:3 / 100,
  in_bench = seq(0.75, 0.95, by = 0.05),
  risk_len = 2:5
)

total_eq <- fun(
  rp_data_eq,
  factor_temp_eq,
  factor_name$factor_eq$risk_name,
  bias = 1:3 / 100,
  in_bench = seq(0.75, 0.95, by = 0.05),
  risk_len = 2:5
)

total_sq_w <- fun(
  rp_data_sq_w,
  factor_temp_sq_w,
  factor_name_w$factor_sq$risk_name,
  bias = 1:3 / 100,
  in_bench = seq(0.75, 0.95, by = 0.05),
  risk_len = 2:5
)


total_sq$result %>% arrange(desc(y_sp))
total_eq$result %>% arrange(desc(y_sp))

total_sq$yield %>% group_by(bias, in_bench, risk_len) %>% 
  subset(trade_dt > 20170101) %>% 
  do(result = to_perform(.$trade_dt, .$zf)) %>% unnest(result) %>% arrange(desc(y_sp))

total_sq$yield %>% subset(bias == 0.02 & in_bench == 0.8 & risk_len == 2) %>% 
  mutate(trade_dt = ymd(trade_dt)) %>% 
  left_join(hs300, by = 'trade_dt') %>% 
  mutate(zf = zf - hs300) %>% 
  ggplot(aes(x = ymd(trade_dt), y = cumprod(zf + 1))) + geom_line()
