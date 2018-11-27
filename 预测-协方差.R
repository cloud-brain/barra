library(tidyverse)
library(lubridate)

source('相关函数.R')
##数据获取----------------
load('yield_data.RData')
load('factor_data_w.RData')
load('rp_ir.RData')


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
##因子协方差波动率预测------------
##获取回归系数及残差项
to_coef_resid <- function(factor_temp, yield_data)
{
  fun <- function(x)
  {
    lma <-
      lm(
        yield ~ .,
        data = select(x,-trade_dt,-wind_code,-float_value,-suspend),
        weight = x$float_value
      )
    coef_data <-
      data.frame(coef_name = names(coefficients(lma)),
                 coef = coefficients(lma)) %>% subset((substr(coef_name, 1, 5) != 'indus') &
                                                            coef_name != '(Intercept)')
    list(coef_data = coef_data, resid = data.frame(
      wind_code = x$wind_code,
      residuals = lma$residuals
    ))
  }
  coef_temp <- yield_data %>% left_join(factor_temp, by = c('trade_dt', 'wind_code')) %>% na.omit
  coef_temp <- coef_temp %>% group_by(trade_dt) %>% 
    do(lm_data = fun(.))
  coef_temp %>% ungroup %>% transmute(trade_dt, coef_data = map(lm_data, ~.$coef_data), resid_data = map(lm_data, ~.$resid))
}

##计算偏离度指标
bias_test <- function(coef_temp, fun)
{
  temp <- coef_temp %>% group_by(coef_name) %>% 
    arrange(coef_name, trade_dt) %>% 
    mutate(sigma = fun(coef) %>% dplyr::lag(1, default = NA))
  temp %>% group_by(trade_dt) %>%
    summarise(bias = sd(coef/sigma))
}

exp_cal <- function(x, y, k)
{
  weight <- (0.5^(1/k))^(length(x):1)
  cumsum((x - cummean(x))*(y - cummean(y)) * weight) / cumsum(weight)
}

coef_sq_w <- to_coef_resid(rp_data_sq_w %>% select(-value), yield_data_w)

get_coef <- function(x)
{
  x %>% select(trade_dt, coef_data) %>% unnest(coef_data)
}

fun <- function(i, windows)
{
  trade_list <- coef_sq_w$trade_dt %>% unique %>% sort
  begin_dt <- dplyr::lag(trade_list, n = windows, default = NULL)[-(1:20)]
  end_dt <- dplyr::lead(trade_list, n = windows, default = NULL)[-(1:20)]

  bias_data <- bias_test(coef_sq_w %>% select(trade_dt, coef_data) %>% unnest(coef_data), fun = function(x) sqrt(exp_cal(x,x, i)))
  output <- data.frame(end_dt,
             pred_bias =
               mapply(function(num) {
                 temp <- bias_data %>% subset(between(trade_dt, begin_dt[num], end_dt[num]))
                 (forecast::auto.arima(temp$bias, allowdrift = F) %>% predict)$pred %>% as.numeric
               }, 1:length(begin_dt)))
  bias_data %>% left_join(output %>% rename(trade_dt = end_dt), by = 'trade_dt') %>% na.omit %>% mutate(pred_bias = lag(pred_bias, 1)) %>% na.omit
}

bias_data <- fun(16, 72)


get_covariance <- function(coef_data, i)
{
  combn_n <- function(x)
  {
    temp <- rbind(combn(x, 2, simplify = F) %>% do.call('rbind', .), matrix(rep(x, each = 2), ncol = 2, byrow = T))
    tibble(coef_1 = temp[,1], coef_2 = temp[,2])
  }
  
  coef_data <- coef_data %>% arrange(coef_name, trade_dt)
  trade_list <- coef_data$trade_dt %>% unique
  variance_name <- combn_n(coef_data$coef_name %>% unique %>% as.character)
  output <- variance_name %>% mutate(value = map2(coef_1, coef_2,
                                                  function(coef_1, coef_2)
                                                    data.frame(
                                                      trade_dt = trade_list,
                                                      value = exp_cal(coef_data$coef[coef_data$coef_name == coef_1],
                                                                      coef_data$coef[coef_data$coef_name == coef_2], i)
                                                    )))
  output %>% unnest(value) %>% nest(coef_1, coef_2, value, .key = 'cov')

}

coef_cov <- get_covariance(get_coef(coef_sq_w), 16)

# mean((temp$bias - 1)^2)
# mean((temp$bias/temp$pred_bias - 1)^2)

# fun <- function(i, windows)
# {
#   trade_list <- coef_sq_w$trade_dt %>% unique %>% sort
#   begin_dt <- dplyr::lag(trade_list, n = windows, default = NULL)[-(1:20)]
#   end_dt <- dplyr::lead(trade_list, n = windows, default = NULL)[-(1:20)]
# 
#   bias_data <- bias_test(coef_sq_w %>% select(trade_dt, coef_data) %>% unnest(coef_data), fun = function(x) exp_cal(x,x, i))
#   output <- data.frame(end_dt,
#              pred_bias =
#                mapply(function(num) {
#                  temp <- bias_data %>% subset(between(trade_dt, begin_dt[num], end_dt[num]))
#                  (forecast::auto.arima(temp$bias, allowdrift = F) %>% predict)$pred %>% as.numeric
#                }, 1:length(begin_dt)))
#   bias_data <- bias_data %>% left_join(output %>% rename(trade_dt = end_dt), by = 'trade_dt') %>% na.omit %>% mutate(pred_bias = lag(pred_bias, 1)) %>% na.omit
#   mean((bias_data$bias / bias_data$pred_bias - 1)^2)
# }
# 
# library(multidplyr)
# cl <- create_cluster(3)
# cl %>% cluster_library('tidyverse') %>% cluster_copy(fun) %>%
#   cluster_copy(coef_sq_w) %>% cluster_copy(bias_test) %>% cluster_copy(exp_cal)
# temp <- expand.grid(gap = seq(10, 30, by = 2), windows = 1:8 * 12) %>% partition(gap, windows, cluster = cl) %>%
#   mutate(bias = map2_dbl(gap, windows, fun)) %>% collect()
# 16, 72是比较合适的参数 


##因子个股波动率预测----------------

get_resid <- function(x)
{
  x %>% select(trade_dt, resid_data) %>% unnest
}

stock_sigma <-
  coef_sq_w %>% get_resid %>% arrange(wind_code, trade_dt) %>% group_by(wind_code) %>%
  mutate(sigma = exp_cal(residuals, residuals, 16)) %>% select(-residuals) %>% ungroup %>%
  nest(wind_code, sigma, .key = 'sigma')

sigma_data <- bias_data %>% select(trade_dt, pred_bias) %>% left_join(coef_cov, by = 'trade_dt') %>% 
  left_join(stock_sigma, by = 'trade_dt')

save(sigma_data, file = 'sigma_data.RData')
