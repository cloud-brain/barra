library(tidyverse)
library(multidplyr)

source('相关函数.R')
##数据获取----------------
load('yield_data.RData')

load('factor_data.RData')
load('factor_data_w.RData')

load('factor_name.RData')
##相关函数-----------
##处理因子数据,按顺序正交
to_factor <- function(factor_data, factor_name)
{
  factor_temp <- factor_data %>% select(trade_dt, wind_code, float_value, one_of(factor_name[1]))
  for(i in 2:length(factor_name))
  {
    temp <- factor_temp %>% left_join(factor_data %>% select(trade_dt, wind_code, one_of(factor_name[i])), by = c('wind_code', 'trade_dt')) %>% 
      group_by(trade_dt) %>% do(value = data.frame(wind_code = .$wind_code, 
                                                   factor_value = orthogon(unlist(.[,i+3]), data.frame(.[,4:(i+2)]), .$float_value))) %>% 
      unnest(value) %>% 
      group_by(trade_dt) %>% 
      mutate(factor_value = ifelse(is.na(factor_value), 0, factor_value)) %>% 
      mutate(factor_value = factor_value / sd(factor_value))
    
    factor_temp <- factor_temp %>% left_join(temp, by = c('wind_code', 'trade_dt'))
    names(factor_temp)[i+3] <- factor_name[i]
  }
  factor_temp
}

##因子数据计算预期收益
to_rp <- function(alpha_name, factor_temp, yield_data)
{
  coef_temp <- yield_data %>% select(-suspend) %>% left_join(factor_temp, by = c('trade_dt', 'wind_code')) %>% na.omit
  coef_temp <- coef_temp %>% group_by(trade_dt) %>% 
    do(lm_data = lm(yield ~ ., data = select(., -trade_dt, -wind_code, -float_value), weight = .$float_value))
  coef_temp <- coef_temp %>% 
    mutate(coef = data.frame(coef_name = names(coefficients(lm_data)), 
                             coef = coefficients(lm_data)) %>% list) %>% 
    select(-lm_data) %>% unnest(coef)
  
  coef_temp <- coef_temp %>% subset(substr(coef_name,1,5) != 'indus')
  
  # coef_temp %>% group_by(coef_name) %>%
  #   summarise(
  #     icir = abs(mean(coef)) / sd(coef) * sqrt(n()),
  #     icir_1y = abs(mean(coef[trade_dt > 20170901])) /
  #       sd(coef[trade_dt > 20170901]) * sqrt(12),
  #     icir_1y_rate = mean(abs(zoo::rollapplyr(coef, fill = NA, width = 12, FUN = function(x) mean(x) / sd(x) * sqrt(12))) > 2, na.rm = T)
  #   ) %>% arrange(desc(icir))
  
  
  temp <- coef_temp %>% subset(coef_name %in% alpha_name) %>% group_by(coef_name) %>%
    arrange(coef_name, trade_dt) %>% mutate(coef = cummean(coef) %>% dplyr::lag(1, default = NA)) %>% na.omit
  
  factor_temp %>% select(trade_dt, wind_code, one_of(alpha_name)) %>% 
    reshape2::melt(id = c('trade_dt', 'wind_code'), variable.name = "coef_name") %>% 
    left_join(temp, by = c('trade_dt', 'coef_name')) %>%  
    group_by(wind_code, trade_dt) %>% 
    summarise(value = sum(value * coef)) %>% na.omit
}

##收益预测评价--------------

##计算回归系数
to_coef <- function(factor_temp)
{
  coef_temp <- yield_data_m %>% left_join(factor_temp, by = c('trade_dt', 'wind_code')) %>% na.omit
  coef_temp <- coef_temp %>% group_by(trade_dt) %>% 
    do(lm_data = lm(yield ~ ., data = select(., -trade_dt, -wind_code, -float_value, -suspend), weight = .$float_value))
  coef_temp <- coef_temp %>% 
    mutate(coef = data.frame(coef_name = names(coefficients(lm_data)), 
                             coef = coefficients(lm_data)) %>% list) %>% 
    select(-lm_data) %>% unnest(coef)
  
  coef_temp <- coef_temp %>% subset(substr(coef_name,1,5) != 'indus')
}

trunc_95 <- function(x)
{
  x[x > quantile(x, 0.95)] <- quantile(x, 0.95)
  x
}

fun <- function(facotr_data, factor_name)
{
  factor_temp_sq <- to_factor(facotr_data, factor_name$factor_name)
  to_coef(factor_temp_sq) %>% subset(coef_name %in% factor_name$alpha_name)
}


coef_sq_w <- fun(factor_data_total_w %>% mutate(float_value = sqrt(float_value)), factor_name_w$factor_sq)
coef_eq <- fun(factor_data_total %>% mutate(float_value = 1), factor_name$factor_eq)
coef_tr <- fun(factor_data_total %>% group_by(trade_dt) %>%
                     mutate(float_value = trunc_95(sqrt(float_value))) %>% ungroup,
               factor_name$factor_tr)

##使用
cal_rmse <- function(coef_temp, fun)
{
  temp <- coef_temp %>% group_by(coef_name) %>%
    arrange(coef_name, trade_dt) %>% mutate(coef_e = fun(coef) %>% dplyr::lag(1, default = NA)) %>% 
    subset(trade_dt > 20110101)
  temp %>% ungroup %>% summarise(rmse = sqrt(mean((coef - coef_e)^2)))  
}

exp_cal <- function(x, k)
{
  weight <- (0.5^(1/k))^(length(x):1)
  cumsum(x*weight) / cumsum(weight)
}

coef_temp <- coef_sq
cal_rmse(coef_temp, fun = function(x) cummean(x))
data.frame(param = 6:20, rmse = mapply(function(i) 
  cal_rmse(coef_temp, fun = function(x) zoo::rollmeanr(x, i, fill = NA)), 6:20) %>% 
             do.call('c',.)) %>% arrange(rmse) %>% head
data.frame(param = 6:100, rmse = mapply(function(i) 
  cal_rmse(coef_temp, fun = function(x) exp_cal(x, i)), 6:100) %>% 
             do.call('c',.))%>% arrange(rmse) %>% head

temp <- coef_eq %>% subset(trade_dt > 20100101) %>% arrange(coef_name, trade_dt) %>% 
  group_by(coef_name) %>% 
  do(lma = forecast::auto.arima(.$coef))
for(i in temp$lma) print(i) 

##结果上显示使用长期的均值效果最佳

##rp计算-------------------
##计算rp
##根号加权
factor_temp_sq <- to_factor(factor_data_total %>% 
                              mutate(float_value = sqrt(float_value)), 
                            factor_name$factor_sq$factor_name)
rp_data_sq <- to_rp(factor_name$factor_sq$alpha_name, factor_temp_sq, yield_data_m) 
rp_data_sq <- rp_data_sq %>% left_join(factor_temp_sq, by = c('trade_dt','wind_code'))

##等权
factor_temp_eq <- to_factor(factor_data_total %>% 
                              mutate(float_value = 1), 
                            factor_name$factor_eq$factor_name)
rp_data_eq <- to_rp(factor_name$factor_eq$alpha_name, factor_temp_eq, yield_data_m) 
rp_data_eq <- rp_data_eq %>% left_join(factor_temp_eq, by = c('trade_dt','wind_code'))

##周度根号加权
factor_temp_sq_w <- to_factor(factor_data_total_w %>% 
                                mutate(float_value = sqrt(float_value)), 
                              factor_name_w$factor_sq$factor_name)
rp_data_sq_w <- to_rp(factor_name_w$factor_sq$alpha_name, factor_temp_sq_w, yield_data_w) 
rp_data_sq_w <- rp_data_sq_w %>% left_join(factor_temp_sq_w, by = c('trade_dt','wind_code'))

##根号加权_5年
factor_temp_sq_5y <- factor_name$factor_sq_5y %>%
  transmute(trade_dt = end_dt, 
            factor_name,
            result = pmap(list(begin_dt, end_dt, factor_name),
                          function(begin_dt, end_dt, factor_name)
                            to_factor(
                              factor_data_total %>% 
                                subset(between(trade_dt, begin_dt, end_dt)) %>%
                                mutate(float_value = sqrt(float_value)),
                              factor_name$factor_name
                            )))

rp_data_sq_5y <- factor_temp_sq_5y %>% 
  transmute(value = pmap(list(factor_name, result, dt = trade_dt), 
                         function(factor_name, result, dt) 
                           to_rp(factor_name$alpha_name, result, yield_data_m) %>% 
                           subset(trade_dt == dt))) %>% unnest

factor_temp_sq_5y <- factor_temp_sq_5y %>% 
  mutate(result = map2(result, trade_dt, function(x,y) x %>% subset(trade_dt == y)))

rp_data_sq_5y <- rp_data_sq_5y %>% nest(wind_code, value, .key = 'value') %>%
  left_join(factor_temp_sq_5y %>% select(trade_dt, result), by = c('trade_dt')) %>% 
  mutate(value = map2(value, result, function(x,y) x %>% left_join(y, by = 'wind_code'))) %>% select(-result)

##根号加权_3年
factor_temp_sq_3y <- factor_name$factor_sq_3y %>%
  transmute(trade_dt = end_dt, 
            factor_name,
            result = pmap(list(begin_dt, end_dt, factor_name),
                          function(begin_dt, end_dt, factor_name)
                            to_factor(
                              factor_data_total %>% 
                                subset(between(trade_dt, begin_dt, end_dt)) %>%
                                mutate(float_value = sqrt(float_value)),
                              factor_name$factor_name
                            )))

rp_data_sq_3y <- factor_temp_sq_3y %>% 
  transmute(value = pmap(list(factor_name, result, dt = trade_dt), 
                         function(factor_name, result, dt) 
                           to_rp(factor_name$alpha_name, result, yield_data_m) %>% 
                           subset(trade_dt == dt))) %>% unnest

factor_temp_sq_3y <- factor_temp_sq_3y %>% 
  mutate(result = map2(result, trade_dt, function(x,y) x %>% subset(trade_dt == y)))

rp_data_sq_3y <- rp_data_sq_3y %>% nest(wind_code, value, .key = 'value') %>%
  left_join(factor_temp_sq_3y %>% select(trade_dt, result), by = c('trade_dt')) %>% 
  mutate(value = map2(value, result, function(x,y) x %>% left_join(y, by = 'wind_code'))) %>% select(-result)


save(rp_data_sq, 
     rp_data_eq, 
     rp_data_sq_w,
     rp_data_sq_5y, 
     rp_data_sq_3y, 
     file = 'rp_ir.RData')
