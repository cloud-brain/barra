library(tidyverse)
library(lubridate)
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


get_perform <- function(x, end_date = NULL)
{
  if(is.null(end_date))
  {
    return(x %>% mutate(result = map(result, ~.$perform)) %>% unnest(result))
  }else{
    return(x %>% 
      mutate(result = map(result, function(x) with(x$yield %>% subset(trade_dt > end_date), 
                                                   to_perform(trade_dt, zf)))) %>% 
      group_by(bias, in_bench, risk_len) %>% 
      unnest(result))
  }
  
}


yield_sd_line <- function(x, ...)
{
  x <- x %>% get_perform(...) 
  x <- x %>% ungroup %>% arrange(desc(y_m)) %>% mutate(num = 1:n())
  i <- 1
  while(i < nrow(x))
  {
    x <- x %>% subset(y_sd < y_sd[i] | num <= num[i])
    i <- i + 1 
  }
  x %>% select(-num)
}

total_eq %>% get_perform %>% arrange(desc(y_sp)) %>% head(5)
total_sq %>% get_perform %>% arrange(desc(y_sp)) %>% head(5)
total_sq_w %>% get_perform %>% arrange(desc(y_sp)) %>% head(5)
total_sq_3y %>% get_perform %>% arrange(desc(y_sp)) %>% head(5)

end_date <- 20120101
rbind(cbind(type = 'eq', yield_sd_line(total_eq, end_date = end_date)),
      cbind(type = 'sq', yield_sd_line(total_sq, end_date = end_date)),
      cbind(type = 'sq_w', yield_sd_line(total_sq_w, end_date = end_date)),
      cbind(type = 'sq_3y', yield_sd_line(total_sq_3y, end_date = end_date)),
      cbind(type = 'sq_5y', yield_sd_line(total_sq_5y, end_date = end_date))) %>% 
  ggplot(aes(x = y_sd, y = y_m, color = type)) + geom_line(size = 2)  

    
(total_sq_3y %>% subset(bias == 0.08 & round(in_bench,2) == 0.950 & risk_len == 10) %>% .$result)[[1]]$yield %>% 
  mutate(trade_dt = ymd(trade_dt)) %>% 
  left_join(hs300, by = 'trade_dt') %>% 
  mutate(zf = zf - hs300) %>% 
  ggplot(aes(x = ymd(trade_dt), y = cumsum(zf))) + geom_line()

(total_sq_3y %>% subset(bias == 0.08 & round(in_bench,2) == 0.950 & risk_len == 10) %>% .$result)[[1]]$yield %>% 
  with(., to_perform(trade_dt, zf, group_year = T))