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
                      where variable_code in (
                      'hcss_ethfrac', 'hcss_relfrac', 'al_ethnic', 'al_religion', 'NY.GDP.PCAP.PP.KD', 'SL.UEM.TOTL.ZS', 'SI.POV.GINI',
                      'SI.POV.DDAY', 'SI.POV.LMIC', 'SI.POV.UMIC', 'SP.DYN.IMRT.IN', 'wbgi_vae', 'bci_bci', 'v2x_execorr', 'v2x_corr',
                      'wbgi_cce') 
                      and year >= 1995 and year <= 2017") %>% 
  mutate_if(bit64::is.integer64, as.integer) %>% 
  distinct(iso3c, variable_code, year_structural, .keep_all = T) %>% 
  filter(iso3c %in% cs$iso3c) %>% 
  spread(variable_code, value) %>% 
  group_by(iso3c) %>% 
  mutate(
    eth = mean(al_ethnic, na.rm = T),
    rel = mean(al_religion, na.rm = T)
  ) %>% 
  mutate(
    hcss_ethfrac = ifelse(is.na(hcss_ethfrac), eth, hcss_ethfrac),
    hcss_relfrac = ifelse(is.na(hcss_relfrac), rel, hcss_relfrac)
  ) %>% 
  select(-eth, -rel)

## reign

reign <- read_tsv("/srv/r-common/data/reign.txt") %>% 
  rename(month_indep = month_event_reign) %>% 
  select(iso3c:month_indep, anticipation, election_recent, governmentDominantParty:governmentWarlordism)

df_full <- left_join(rack, df) %>% 
  left_join(reign)

write_tsv(df_full, "data/structural_data.txt.gz")
