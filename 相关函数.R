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

##最优组合求解
best_pfp_bench <- function(r_exp, x_beta, risk_matrix = 0, srisk, benchmark)
{
  require(quadprog)
  nfactor <- ncol(x_beta)
  x_beta <- as.matrix(x_beta)
  risk_matrix <- as.matrix(risk_matrix)
  
  ##累计权重限制
  weight <- rep(1, nrow(x_beta))
  weight_total <- 1
  
  ##beta限制
  beta_expo <- x_beta  ##应剔除一个行业，限制完全共线性
  beta_total <- t(x_beta) %*% benchmark ##风格暴露与基准一致 
  
  ##个股权重限制
  stock_w <- diag(rep(1, nrow(x_beta)))
  stock_total <- rep(0, nrow(x_beta))
  
  dmat <- diag(srisk) + x_beta %*% risk_matrix %*% t(x_beta) 
  dvec <- 0.5 * r_exp + diag(srisk) %*% benchmark
  amat <- cbind(weight, beta_expo, stock_w) %>% as.matrix
  bvec <- c(weight_total, beta_total, stock_total)
  
  output <- solve.QP(Dmat = dmat, dvec = dvec, 
                     Amat = amat, bvec = bvec, meq = ncol(x_beta) + 1)
  output$solution
}

##天风组合求解
tf_pfp_bench <- function(r_exp, x_beta, benchmark, bias, in_bench)
{
  require(ROI)
  # require(limSolve)
  # num <- nrow(x_beta)
  # x_beta <- as.matrix(x_beta)
  # 
  # ##等式
  # ##累计权重限制
  # weight <- rep(1, num)
  # weight_total <- 1
  # 
  # ##beta限制
  # beta_expo <- x_beta  ##应剔除一个行业，限制完全共线性
  # beta_total <- t(x_beta) %*% benchmark ##风格暴露与基准一致
  # 
  # ##不等式
  # ##个股大于基准-0.05(w > w_b - 0.05)
  # stock_lower <- diag(num)
  # stock_w_lower <- benchmark - bias
  # 
  # ##个股小于基准+0.05(-w > -w_b - 0.05)
  # stock_upper <- -diag(num)
  # stock_w_upper <- -benchmark - bias
  # 
  # ##个股在指数范围内(sum(w) > in_bench)
  # stock_index <- ifelse(benchmark == 0 ,0, 1)
  # weight_index <- in_bench
  # 
  # E <- rbind(weight, t(beta_expo))
  # f <- c(weight_total, beta_total)
  # G <- rbind(stock_lower, stock_upper, stock_index) %>% as.matrix
  # h <- c(stock_w_lower, stock_w_upper, weight_index)
  # 
  # output <- linp(E = E, F = f, G = G, H = h, Cost = -r_exp,
  #                ispos = T)
  # output$X
  x_beta <- x_beta[,!(colMeans(x_beta == 0) == 1)]
  num <- length(r_exp)
  ##beta限制
  beta_con <- L_constraint(t(x_beta), dir = rep('==', ncol(x_beta)), rhs = rep(0, ncol(x_beta)))
  ##合计为0
  add_con <- L_constraint(matrix(rep(1, num), nrow = 1), '==', rhs = 0)
  ##指数范围
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
barra_pfp_bench <- function(r_exp, x_beta, risk_matrix = 0, srisk, benchmark, sig_thres = 9)
{
  require(cccp)
  num <- length(srisk)
  x_beta <- as.matrix(x_beta)
  
  ##新等式
  ##线性等式约束
  ###权重为0
  weight <- rep(1, num)
  weight_total <- 0
  ###beta限制
  beta_expo <- x_beta  ##应剔除一个行业，避免完全共线性
  beta_total <- rep(0, ncol(beta_expo))
  ###综合
  A <- t(cbind(weight, beta_expo) %>% as.matrix)
  b <- c(weight_total, beta_total)
  
  ##不等式约束
  ###个股权重限制
  stock_weights <- nnoc(G = -diag(nrow(x_beta)), h = benchmark)
  ###波动率限制
  sigma_ths <- socc(F = chol(diag(srisk^2) + risk_matrix), g = rep(0,num), d = rep(0,num), f = sig_thres)
  
  output <- cccp(q = -r_exp, A = A, b = b, cList = list(stock_weights, sigma_ths), optctrl = ctrl(trace = F, reltol = 1e-5, abstol = 1e-05, feastol = 1e-05))
  
  getx(output) + benchmark
}

##barra组合
barra_pfp_bench <- function(r_exp, x_beta, risk_matrix = NULL, srisk, benchmark, sig_thres = 9)
{
  num <- length(srisk)
  x_beta <- as.matrix(x_beta)
  

  ###风险因子及权重限制
  beta <- cbind(rep(1, num), beta_expo) %>% as.matrix
  
  ##协方差矩阵
  if(is.null(risk_matrix))
  {
    risk_matrix <- diag(1/srisk)
  }else{
    risk_matrix <- solve(risk_matrix + diag(srisk))
  }
  
  ##求解公式
  w <- risk_matrix %*% (r_exp - beta %*% solve(t(beta) %*% risk_matrix %*% beta) %*% t(beta) %*% risk_matrix %*% r_exp)
  
  ##约束范围
  adj_param <- sqrt(sig_thres / (t(w) %*% solve(risk_matrix) %*% w))
  w <- adj_param[1,1] * w
  
  which(w + benchmark < 0)[1]
  
  getx(output) + benchmark
}

##icir
ic_test <- function(trade_dt, f_data, yield_data)
{
  temp <- data.frame(trade_dt, x = f_data, y = yield_data) %>% group_by(trade_dt) %>% 
    do(ic_model = cor.test(.$x, .$y))
  output <- temp %>% transmute(trade_dt, ic_value = ic_model$estimate, ic_pvalue = ic_model$p.value)
  print(output %>% mutate(trade_dt =  ymd(trade_dt)) %>% reshape2::melt(id = 'trade_dt') %>%
    ggplot(aes(x = trade_dt, y = value)) + geom_line() + facet_grid(variable~., scale = 'free'))
  return(output)
}

##wind代码转换
to_wind_code <- function(x)
{
  paste0(x, ifelse(substr(x,1,1) == '6', '.SH', '.SZ'))
}


