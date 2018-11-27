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
#@param cov, tibble, 协方差矩阵
#@param factor_name, list, 风险因子名称
#@param bias 偏离
to_weight <- function(x, cov, factor_name, bias)
{
  ##转化行业数据为矩阵并剔除一个行业
  risk_name <- setdiff(factor_name$risk_name, 'indus')
  risk_total <- data.frame(x %>% select(one_of(risk_name)), 
                             model.matrix( ~ indus + 0, data = x))
  
  ##构建协方差矩阵
  risk_matrix <- cov %>% spread(coef_2, value)
  risk_matrix <- risk_matrix %>% subset(coef_1 %in% factor_name$alpha_name) %>% 
    select(coef_1, one_of(factor_name$alpha_name))
  risk_matrix$coef_1 <- factor(risk_matrix$coef_1, levels = factor_name$alpha_name)
  risk_matrix <- risk_matrix %>% arrange(coef_1) %>% select(-coef_1)
  risk_matrix[is.na(risk_matrix)] <- 0
  temp <- risk_matrix
  diag(temp) <- 0
  risk_matrix <- as.matrix(risk_matrix + t(temp))
  
  
  
  ##计算权重
  tibble(
         wind_code = x$wind_code,
         weight = barra_pfp_bench(
           r_exp = x$value,
           x_beta = risk_total,
           x_alpha = as.matrix(x %>% select(one_of(factor_name$alpha_name))),
           risk_matrix = risk_matrix,
           srisk = x$sigma,
           benchmark = x$weight, 
           bias = bias
         )
       ) %>% subset(abs(weight) > 0.00001)
}


##组合求解---------------
load('rp_ir.RData')
load('factor_name.RData')
load('sigma_data.RData')

fun_nest_p <- function(rp_data, risk_name, sigma_data, index_weight, bias, cl_len = 2, weight_save = T)
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
  
  ##合并波动率
  rp_data <-
    rp_data %>% subset(trade_dt > min(sigma_data$trade_dt)) %>% 
    left_join(sigma_data, by = 'trade_dt') %>%
    mutate(value = map2(value, sigma, function(x, y)
      left_join(x, y, by = 'wind_code'))) %>% select(-sigma)
  
  
  ##调整因子名称与风险因子日期一致
  risk_name <- risk_name %>% group_by(begin_dt, end_dt) %>% 
    mutate(trade_dt = list(tibble(trade_dt = rp_data$trade_dt) %>% subset(trade_dt <= end_dt & trade_dt > begin_dt))) %>% 
    ungroup %>% select(trade_dt, end_dt, factor_name) %>% 
    unnest(trade_dt, .drop = F) %>% 
    group_by(trade_dt) %>% filter(end_dt == min(end_dt)) %>% select(-end_dt)
  
  ##合并风险因子名称
  rp_data <- rp_data %>% left_join(risk_name, by = 'trade_dt')
  
  rp_data <- rp_data %>% subset(trade_dt != 20181011)
  
  fun <- function(bias)
  {
    temp_w <- rp_data %>%
      transmute(trade_dt,
                weight = pmap(list(value, cov, factor_name), function(x, y, z)
                  to_weight(
                    x,
                    cov = y,
                    factor_name = z,
                    bias = bias
                  ))) %>% unnest
    
    ##计算净值
    temp_y <- temp_w %>% with(., to_yield(trade_dt, wind_code, weight))
    list(weight = temp_w, yield = temp_y)
  }
  
  
  param <- expand.grid(bias = bias)
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
    
    temp_total <- param %>% partition(bias, cluster = cl) %>% 
      mutate(result = map(bias, fun)) %>% collect()
  }else{
    temp_total <- param %>% group_by(bias) %>%
      mutate(result = map(bias, fun))
  }
  
  temp_total %>% mutate(result = map(result, function(x)
    c(x,
      list(
        perform = with(x$yield %>% arrange(trade_dt),
                       to_perform(trade_dt, zf))
      ))))
}

total_sq_w <- fun_nest_p(
  rp_data_sq_w,
  factor_name$factor_sq_w,
  sigma_data = sigma_data,
  index_weight = index_weight %>% subset(index_code == '000300.SH') %>% select(-index_code),
  bias = 1:10 / 100,
  cl_len = 1
)


save(total_sq_w, file = 'result_weight_qp.RData')
