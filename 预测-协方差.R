library(dplyr)
library(tidyr)

source('相关函数.R')
##数据获取----------------
load('yield_data.RData')
load('factor_data.RData')
load('rp_ir.RData')


##因子计算-------------------
##处理因子数据
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

##因子异方差检验--------------
bp_data_fun <- function(yield_data, factor_temp, factor_data)
{
  factor_data <- factor_temp %>% select(-float_value) %>% 
    left_join(factor_data %>% select(wind_code, trade_dt, float_value), by = c('wind_code', 'trade_dt'))
  factor_data <- yield_data %>% left_join(factor_data, by = c('trade_dt', 'wind_code')) %>% na.omit
  coef_temp <- factor_data %>% group_by(trade_dt) %>%
    do(lm_data = lm(yield ~ ., data = select(., -trade_dt, -wind_code, -float_value), weight = .$float_value))
  coef_temp %>% mutate(bptest = lmtest::bptest(lm_data)$p.value) %>% select(-lm_data)
}

coef_temp_tv <- bp_data_fun(yield_data_m, factor_temp_sq, factor_data_total %>% mutate(float_value = sqrt(float_value)))
coef_temp_eq <- bp_data_fun(yield_data_m, factor_temp_eq, factor_data_total %>% mutate(float_value = 1))

coef_temp_tv %>% rename(bptest_tv = bptest) %>%
  left_join(coef_temp_eq %>% rename(bptest_eq = bptest), by = 'trade_dt') %>%
  mutate(bp_diff = bptest_tv - bptest_eq) %>% summary

##比较异方差差异
t.test(coef_temp_eq$bptest, coef_temp_tv$bptest)
##因子波动率预测------------
to_coef <- function(factor_temp)
{
  coef_temp <- yield_data_m %>% left_join(factor_temp, by = c('trade_dt', 'wind_code')) %>% na.omit
  coef_temp <- coef_temp %>% group_by(trade_dt) %>% 
    do(lm_data = lm(yield ~ ., data = select(., -trade_dt, -wind_code, -float_value, -suspend), weight = .$float_value))
  coef_temp <- coef_temp %>% 
    mutate(coef = data.frame(coef_name = names(coefficients(lm_data)), 
                             coef = coefficients(lm_data)) %>% list) %>% 
    select(-lm_data) %>% unnest(coef)
  
  coef_temp <- coef_temp %>% subset((substr(coef_name,1,5) != 'indus') & coef_name != '(Intercept)')
}

coef_sq <- to_coef(factor_temp_sq)
coef_eq <- to_coef(factor_temp_eq)

bias_test <- function(coef_temp, fun)
{
  temp <- coef_temp %>% group_by(coef_name) %>% 
    arrange(coef_name, trade_dt) %>% 
    mutate(sigma = fun(coef) %>% dplyr::lag(1, default = NA))
  temp %>% group_by(coef_name) %>%
    arrange(coef_name, trade_dt) %>% 
    mutate(bias = zoo::rollapplyr(coef/sigma, width = 12, 
                             FUN = function(x) sqrt(sum((x - mean(x))^2)/11), fill = NA)) %>% 
    na.omit
}
exp_cal <- function(x, y, k)
{
  weight <- (0.5^(1/k))^(length(x):1)
  sqrt(cumsum((x - cummean(x))*(y - cummean(y)) * weight) / cumsum(weight))
}


temp2 <- bias_test(coef_sq, fun = function(x) exp_cal(x,x, 20))
# 20附近最佳
# data.frame(param = 10:100, 
#            bias = mapply(function(i) (bias_test(coef_sq, fun = function(x) exp_cal(x,x, i)) %>% subset(trade_dt > 20100101))$bias %>% mean, 10:100))


exp_ar_adj <- function(x, k)
{
  sigma <- exp_cal(x, x, k)
  bias <-  zoo::rollapplyr(x/dplyr::lag(sigma, 1, default = NA), width = 12, 
                           FUN = function(x) sqrt(sum((x - mean(x))^2)/11), fill = NA)
  num <- 12+11
  adj_factor <- c(rep(NA, num), mapply(function(i) predict(forecast::auto.arima(bias[1:i], allowdrift = F), 1)$pred %>% as.double, num:(length(bias)-1)))
  sigma * adj_factor
}

temp <- bias_test(coef_sq, fun = function(x) exp_ar_adj(x, 20))
