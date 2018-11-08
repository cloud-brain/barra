library(tidyverse)
library(lubridate)
library(multidplyr)

source('相关函数.R')
##相关函数----------
##正交因子池后计算因子的收益序列
##外生变量(无)
##参数
##@cl:并行接口
##@total_data: 因子数据
##@yield_data: 与因子频率对应的收益率数据
##@factor_name: 因子池
##@keep：不进行正交的因子数量
get_ic <- function(cl, total_data, yield_data, factor_name, keep = 1)
{
  require(parallel)
  ##因子数量大于固定数量时按因子进入次序正交
  if(length(factor_name) > keep)
  {
    factor_temp <- total_data %>% select(trade_dt, wind_code, float_value, one_of(factor_name[1:keep]))
    for(i in (1+keep):length(factor_name))
    {
      temp <- factor_temp %>% left_join(total_data %>% select(trade_dt, wind_code, one_of(factor_name[i])), by = c('wind_code', 'trade_dt')) %>% 
        group_by(trade_dt) %>% do(value = data.frame(wind_code = .$wind_code, 
                                                     factor_value = orthogon(unlist(.[,i+3]), data.frame(.[,4:(i+2)]), .$float_value))) %>% 
        unnest(value) %>% 
        group_by(trade_dt) %>% 
        mutate(factor_value = ifelse(is.na(factor_value), 0, factor_value)) %>% 
        mutate(factor_value = factor_value / sd(factor_value))
      
      factor_temp <- factor_temp %>% left_join(temp, by = c('wind_code', 'trade_dt'))
      names(factor_temp)[i+3] <- factor_name[i]
    }
  }else{
    factor_temp <- total_data %>% select(trade_dt, wind_code, float_value, one_of(factor_name))
  }
  
  ##并入收益率
  yield_temp <- yield_data %>% 
    left_join(factor_temp %>% select(trade_dt, wind_code, float_value, one_of(factor_name)),
              by = c('wind_code', 'trade_dt')) %>% subset(suspend == 0) %>% na.omit
  
  ##计算因子收益
  ##外部参数
  ##@total_data 待处理因子
  ##@factor_temp 因子池因子
  ##@yield_temp 修正后收益
  ##@orthogon 正交函数
  ##@factor_name 因子名称
  fun <- function(x)
  {
    ##对因子剔除共线性
    output_temp <- total_data %>% select(trade_dt, wind_code, one_of(x)) %>% 
      left_join(factor_temp, by = c('wind_code', 'trade_dt')) %>% 
      group_by(trade_dt) %>% do(value = data.frame(wind_code = .$wind_code, 
                                                   factor_value = orthogon(unlist(.[,x]), data.frame(.[,factor_name]), .$float_value))) %>% 
      unnest(value)
    
    ##填补缺失值并标准化
    output_temp <- output_temp %>% group_by(trade_dt) %>% 
      mutate(factor_value = ifelse(is.na(factor_value), 0, factor_value)) %>% 
      mutate(factor_value = factor_value / sd(factor_value))
    output_temp <- yield_temp %>% inner_join(output_temp, by = c('trade_dt', 'wind_code'))
    
    ##与收益率回归，提取系数
    output_temp <- output_temp %>% group_by(trade_dt) %>% 
      do(lm_data = lm(yield ~ ., data = select(., yield, factor_value, one_of(c(factor_name))), weight = .$float_value))
    output_temp <- output_temp %>% mutate(adj_r = summary(lm_data)$adj.r.squared, 
                                          coef = coefficients(lm_data)['factor_value']) %>% 
      select(-lm_data)
    data.frame(type = x, output_temp)
    # print(i)
  }
  
  cl %>% cluster_library(c('dplyr','tidyr')) %>% cluster_copy(total_data) %>% 
    cluster_copy(factor_temp) %>% cluster_copy(orthogon) %>% 
    cluster_copy(factor_name)
  
  output <- parLapplyLB(cl, setdiff(names(total_data), c('trade_dt', 'wind_code', 'float_value',factor_name)), fun) %>%
    do.call('rbind', .)
  
  output
}

##正交因子池后计算因子的收益序列
##外生变量(无)
##参数
##@factor_data: 因子数据
##@yield_data: 与因子频率对应的收益率数据
##@cl_len: 并行数量
##@max_len: 最多挑选因子数量
##@alpha：是否优先提取阿尔法因子
##@alpha_show：是否展示阿尔法因子数据
##@icir_1y_rate_th: 要求胜率的阈值
get_factor <- function(factor_data, yield_data, cl_len = 2, max_len = 10, alpha = T, alpha_show = F, icir_1y_rate_th = 0.75, begin_dt = min(factor_data$trade_dt), end_dt = max(factor_data$trade_dt))
{
  cl <- create_cluster(cl_len)
  factor_data <- factor_data %>% subset(between(trade_dt, begin_dt, end_dt))
  i <- 1
  factor_name <- 'indus'
  alpha_name <- c()
  ##当前最大修正解释度
  adj_og <- 0
  ##获取时间序列计算一年的周期数量
  trade_dt_list <- unique(factor_data$trade_dt)
  num_1y <- sum(trade_dt_list <= end_dt & trade_dt_list > (end_dt - 10000))
  
  while(i <= max_len)
  {
    f_list <- get_ic(cl, factor_data, yield_data, factor_name)
    temp <- f_list %>% group_by(type) %>%
      summarise(
        adj_r = mean(adj_r),
        icir = mean(coef) / sd(coef) * sqrt(n()),
        icir_1y = mean(coef[trade_dt > end_dt - 10000]) /
          sd(coef[trade_dt > end_dt - 10000]) * sqrt(num_1y),
        icir_1y_rate = mean(abs(zoo::rollapplyr(coef, fill = NA, width = num_1y, FUN = function(x) mean(x) / sd(x) * sqrt(num_1y))) > 2, na.rm = T)
      )
    
    if(alpha)
    {
      if(alpha_show)
      {
        print(temp %>% arrange(desc(icir)) %>% head)
      }
      temp_alpha <- temp %>% arrange(desc(abs(icir))) %>% subset(abs(icir) > 2 & abs(icir_1y) > 2 & icir_1y_rate > icir_1y_rate_th) %>% arrange(desc(icir_1y_rate))
      if(nrow(temp_alpha) > 0)
      {
        factor_name <- c(factor_name, temp_alpha$type[1] %>% as.character)
        alpha_name <- c(alpha_name, temp_alpha$type[1] %>% as.character)
        adj_og <- temp_alpha$adj_r[1]
        print(sprintf('%d times:', i))
        print('add alpha');print(temp_alpha %>% head(3))
        print(paste0('factor is ', paste0(factor_name, collapse = ',')))
        next
      }
    }
    temp_adj <- temp %>% arrange(desc(adj_r))
    factor_name <- c(factor_name, temp_adj$type[1] %>% as.character)
    if(temp_adj$adj_r[1] >= adj_og)
    {
      adj_og <- temp_adj$adj_r[1]
    }else{
      break
    }
    print(sprintf('%d times:', i))
    print('add factor');print(temp_adj %>% head(3))
    print(paste0('factor is ', paste0(factor_name, collapse = ',')))
    i <- i + 1
  }
  if(alpha)
  {
    return(list(factor_name = factor_name, alpha_name = alpha_name, risk_name = setdiff(factor_name, alpha_name)))
  }
  return(factor_name)
}

##icir计算-------------------
load('yield_data.RData')
load('factor_data.RData')
##全市场

trunc_95 <- function(x)
{
  x[x > quantile(x, 0.95)] <- quantile(x, 0.95)
  x
}

##根号加权
output <- get_factor(factor_data_total %>% mutate(float_value = sqrt(float_value)), yield_data_m)
factor_name <- list(factor_sq = output)

##等权
output <- get_factor(factor_data_total %>% mutate(float_value = 1), yield_data_m)
factor_name <- c(factor_name, list(factor_eq = output))

##根号截尾
output <- get_factor(factor_data_total %>% group_by(trade_dt) %>%
                       mutate(float_value = trunc_95(sqrt(float_value))) %>% ungroup,
                     yield_data_m)
factor_name <- c(factor_name, list(factor_tr = output))

##沪深300根号加权
output <- get_factor(factor_data_hs300 %>% mutate(float_value = sqrt(float_value)), yield_data_m, alpha_show = T, icir_1y_rate_th = 0.5)
factor_name <- c(factor_name, list(factor_sq_hs300 = output))

##中证800根号加权
output <- get_factor(factor_data_zz800 %>% mutate(float_value = sqrt(float_value)), yield_data_m, alpha_show = T, icir_1y_rate_th = 0.5)
factor_name <- c(factor_name, list(factor_sq_zz800 = output))


##周度计算
load('factor_data_w.RData')
##全市场
##根号加权
output <- get_factor(factor_data_total_w %>% mutate(float_value = sqrt(float_value)), yield_data_w, alpha_show = T)
factor_name_w <- list(factor_sq = output)


##icir(滚动期)----------------------------------
load('yield_data.RData')
load('factor_data.RData')
##全市场

##根号加权
trade_list <- seq(ymd(20080101), ymd(20180101), by = 'month')
windows <- 36
begin_dt <- trade_list[1:(length(trade_list) - windows)] %>% format('%Y%m%d') %>% as.integer
end_dt <-  trade_list[(windows + 1):length(trade_list)] %>% format('%Y%m%d') %>% as.integer

result <- tibble()
for(i in 58:length(begin_dt))
{
  output <- get_factor(factor_data_total %>% mutate(float_value = sqrt(float_value)), 
                       yield_data_m,
                       begin_dt = begin_dt[i],
                       end_dt = end_dt[i],
                       cl_len = 3)
  result <- rbind(result, 
                  tibble(begin_dt = begin_dt[i], end_dt = end_dt[i],
                         factor_name = list(output)))
  gc(T)
}


