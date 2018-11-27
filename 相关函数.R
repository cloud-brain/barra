##因子预处理--------

##去极值并标准化
extreme_scale <- function(x)
{
  rank_p <- function(x)
  {
    if(length(x) == 1)
    {
      return(0)
    }else{
      return(rank(x, na.last = 'keep') / (length(x) + 1))
    }
  }
  m_value <- median(x, na.rm = T)
  mad <- median(abs(x - m_value), na.rm = T)
  upper <- m_value + 3 * 1.483 * mad
  lower <- m_value - 3 * 1.483 * mad
  if(any(x > upper, na.rm = T))
    x[x > upper & !is.na(x)] <- m_value + (3 + rank_p(x[x > upper & !is.na(x)]) * 0.5) * 1.483 * mad
  if(any(x < lower, na.rm = T))
    x[x < lower & !is.na(x)] <- m_value + (-3.5 + rank_p(x[x < lower & !is.na(x)]) * 0.5) * 1.483 * mad
  
  x
}

extreme_scale_new <- function(x)
{
  fun <- function(x)
  {
    m_value <- median(x, na.rm = T)
    mad <- median(abs(x - m_value), na.rm = T)
    upper <- m_value + 4 * 1.483 * mad
    lower <- m_value - 4 * 1.483 * mad
    which(x > upper | x < lower)
  }
  
  rank_p <- function(x)
  {
    if(length(x) == 1)
    {
      return(0)
    }else{
      return(rank(x, na.last = 'keep') / (length(x) + 1))
    }
  }
  
  temp_value <- data.frame(pos = 0:100, value = quantile(x, 0:100/100, na.rm = T)) %>% 
    mutate(dif_value = c(NA,diff(value)))
  max_adj <- 6
  if(length(fun(temp_value$dif_value)) > max_adj)
  {
    temp <- temp_value %>% arrange(desc(dif_value)) %>% head(max_adj) %>% arrange(pos)
    if(any(temp$pos < 50))
    {
      temp_high <- max((temp %>% subset(pos < 50))$pos)
      x[x < temp_value$value[temp_value$pos == temp_high] & !is.na(x)] <- 
        (temp_value$value[temp_value$pos == temp_high]  - temp_high * temp_value$dif_value[temp_value$pos == temp_high + 1]) + 
        rank_p(x[x < temp_value$value[temp_value$pos == temp_high] & !is.na(x)]) * 
        temp_high * 
        temp_value$dif_value[temp_value$pos == temp_high + 1]
    }
    if(any(temp$pos > 50))
    {
      temp_low <- min((temp %>% subset(pos > 50))$pos)
      x[x > temp_value$value[temp_value$pos == temp_low - 1] & !is.na(x)] <- 
        temp_value$value[temp_value$pos == temp_low - 1] + 
        rank_p(x[x > temp_value$value[temp_value$pos == temp_low - 1] & !is.na(x)]) * 
        (101 - temp_low) * 
        temp_value$dif_value[temp_value$pos == temp_low - 1]
    }
    
    
    
  }else{
    if(length(fun(temp_value$dif_value)) != 0)
    {
      temp <- temp_value[fun(temp_value$dif_value), ]
      if(any(temp$pos < 50))
      {
        temp_high <- max((temp %>% subset(pos < 50))$pos)
        x[x < temp_value$value[temp_value$pos == temp_high] & !is.na(x)] <- 
          (temp_value$value[temp_value$pos == temp_high]  - temp_high * temp_value$dif_value[temp_value$pos == temp_high + 1]) + 
          rank_p(x[x < temp_value$value[temp_value$pos == temp_high] & !is.na(x)]) * 
          temp_high * 
          temp_value$dif_value[temp_value$pos == temp_high + 1]
      }
      if(any(temp$pos > 50))
      {
        temp_low <- min((temp %>% subset(pos > 50))$pos)
        x[x > temp_value$value[temp_value$pos == temp_low - 1] & !is.na(x)] <- 
          temp_value$value[temp_value$pos == temp_low - 1] + 
          rank_p(x[x > temp_value$value[temp_value$pos == temp_low - 1] & !is.na(x)]) * 
          (101 - temp_low) * 
          temp_value$dif_value[temp_value$pos == temp_low - 1]
      }
    }
  }
  x
}

##正交化处理
orthogon <- function(x, f_matrix, weight)
{
  if(any(is.na(x)))
  {
    ##需要对空值进行保留
    require(dplyr)
    temp_lm <- !is.na(x)
    output <- rep(NA, length(x))
    output[temp_lm] <- lm(x~., data = data.frame(x, f_matrix)[temp_lm, ], weights = weight[temp_lm])$residuals %>% unname
    return(output)
  }else{
    return(lm(x~., data = data.frame(x, f_matrix), weights = weight)$residuals %>% unname)
  }
}


##最小波动纯因子组合
min_vol_pfp <- function(x_alpha, x_beta, srisk, risk_matrix = NULL)
{
  sigma <- diag(1/srisk)
  beta <- as.matrix(cbind(x_beta, x_alpha, rep(1, nrow(x_beta))))
  cons <- c(rep(0, ncol(beta) - 1), 1, 0)
  sigma %*% beta %*% solve(t(beta) %*% sigma %*% beta) %*% cons
}

##天风组合求解
tf_pfp_bench <- function(r_exp, x_beta, benchmark, bias, in_bench)
{
  require(ROI)
  x_beta <- x_beta[,!(colMeans(x_beta == 0) == 1)]
  num <- length(r_exp)
  ##beta限制
  beta_con <- L_constraint(t(x_beta), dir = rep('==', ncol(x_beta)), rhs = rep(0, ncol(x_beta)))
  ##合计为0
  add_con <- L_constraint(matrix(rep(1, num), nrow = 1), '==', rhs = 0)
  ##指数范围内权重
  index_con <- L_constraint(matrix(ifelse(benchmark == 0 ,0, 1), nrow = 1), '>=', rhs = in_bench - 1)

  ##求解范围
  x_bound <- V_bound(li = 1:num, lb = -benchmark,
                     ud = bias, nobj = num)

  op <- OP(
    objective = r_exp,
    constraints = rbind(beta_con, add_con, index_con),
    bounds = x_bound,
    maximum = T
  )
  ROI_solve(op, solver = "lpsolve")$solution  + benchmark
}

##barra组合
barra_pfp_bench <- function(r_exp, x_beta, x_alpha, risk_matrix = 0, srisk, benchmark, bias)
{
  require(ROI)
  x_beta <- x_beta[,!(colMeans(x_beta == 0) == 1)]
  num <- length(r_exp)
  ##beta限制
  beta_con <- L_constraint(cbind(t(x_beta), 0, 0), dir = rep('==', ncol(x_beta)), rhs = rep(0, ncol(x_beta)))
  ##合计权重为0
  add_con <- L_constraint(matrix(c(rep(1, num),0,0), nrow = 1), '==', rhs = 0)
  ##偏离情况
  ##调整为两项和
  l1 <- -rbind(c(rep(0, num), 1, 0), cbind(chol(risk_matrix) %*% t(x_alpha), 0, 0)) 
  l2 <- -rbind(c(rep(0, num), 0, 1), cbind(diag(sqrt(srisk)), 0, 0))
  l3 <- -rbind(rep(0, num + 2), c(rep(0, num), 1, 0), c(rep(0, num), 0, 1))
  sigma_con <- C_constraint(rbind(l1, l2, l3), cones = K_soc(c(nrow(l1), nrow(l2), nrow(l3))), rhs = c(rep(0, nrow(l1)), rep(0, nrow(l2)), c(bias, 0, 0)))
  
  ##求解范围
  x_bound <- V_bound(li = 1:(num+2), lb = c(-benchmark, -Inf, -Inf),
                     ui = 1:(num+2), ub = c(rep(1, num), rep(Inf, 2)),
                     nobj = num+2)
  
  op <- OP(
    objective = c(r_exp, 0, 0),
    constraints = rbind(beta_con, add_con, sigma_con),
    bounds = x_bound,
    maximum = T
  )
  ROI_solve(op, solver = "ecos")$solution[1:num] + benchmark
}

##wind代码转换
to_wind_code <- function(x)
{
  paste0(x, ifelse(substr(x,1,1) == '6', '.SH', '.SZ'))
}


