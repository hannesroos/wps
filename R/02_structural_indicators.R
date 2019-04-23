rm(list = ls())

library(tidyverse)
library(odbc)
library(lubridate)

con <- dbConnect(odbc::odbc(), "dwh")

rack <- read_tsv("data/rack.txt.gz") %>% 
  mutate(year_structural = year(month_dependent - years(2)))

cs <- distinct(rack, iso3c)

df <- dbGetQuery(con, 
                      "select iso3c, variable_code, year as year_structural, value from time.annual 
                      where variable_code in ('hcss_ethfrac', 'hcss_relfrac', 'NY.GDP.PCAP.PP.KD', 'SL.UEM.TOTL.ZS', 'SI.POV.GINI',
                      'SI.POV.DDAY', 'SI.POV.LMIC', 'SI.POV.UMIC', 'wbgi_vae', 'bci_bci', 'v2x_execorr', 'v2x_corr', 'wbgi_cce') 
                      and year >= 1995 and year <= 2017") %>% 
  mutate_if(bit64::is.integer64, as.integer) %>% 
  distinct(iso3c, variable_code, year_structural, .keep_all = T) %>% 
  filter(iso3c %in% cs$iso3c) %>% 
  spread(variable_code, value)

latest <- df %>% filter(year_structural == max(year_structural))
oldest <- df %>% filter(year_structural == min(year_structural))

df_full <- left_join(rack, df)

write_tsv(df_full, "data/structural_data.txt.gz")
