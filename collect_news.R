library(koscrap)
library(tidyverse)
library(readxl)

##==============================================================================
## 메타정보 로드
##==============================================================================
meta_file <- "meta/news_keywords.xlsx"
meta_keyword <- readxl::read_xlsx(meta_file, sheet = 2) 

target_id <- meta_keyword |> 
  filter(use_flag == "Y") |> 
  distinct(keyword_id) |> 
  pull() 


##==============================================================================
## 뉴스 목록 수집
##==============================================================================
news_list <- target_id |> 
  purrr::map_df(function(id) {
    Sys.sleep(2)
    keyword <- meta_keyword |> 
      filter(keyword_id == id) |> 
      filter(use_flag == "Y") |> 
      mutate(keyword = ifelse(atomic_flag == "Y", glue::glue("\"{keyword}\""),keyword)) |> 
      pull(keyword)
    
    result_partial <- keyword |> 
      purrr::map_df(function(x) {
        search_result <- koscrap::search_naver(
          query = x,
          type = "news",
          sort = "date",
          do_done = TRUE,
          max_record = 500L
        ) |> 
          bind_cols(keyword_id = id, keyword = x) 
      })
  })

info_scrap <- news_list |> 
  filter(as.Date(publish_date) == "2025-04-28") |> 
  mutate(keyword = str_remove_all(keyword, "\"")) |>
  mutate(is_naver = ifelse(str_detect(link, "n.news.naver.com"), "Y", "N")) |>
  group_by(keyword_id, keyword) |> 
  summarise(
    count = n(),
    count_naver = sum(is_naver == "Y"),
    .groups = "drop"
  ) |>
  right_join(meta_keyword, by = c("keyword_id", "keyword")) |> 
  mutate(count = ifelse(is.na(count), 0, count),
         count_naver = ifelse(is.na(count_naver), 0, count_naver)) |> 
  select(keyword_id, keyword, count, count_naver)

result_stat <- target_id |> 
  purrr::map_df(function(id) {
    news_list |> 
      filter(keyword_id == id) |> 
      filter(as.Date(publish_date) == "2025-04-28") |> 
      mutate(is_naver = ifelse(str_detect(link, "n.news.naver.com"), "Y", "N")) |>
      distinct() |> 
      group_by(keyword_id) |> 
      summarise(distinct_cnt = n(),
                distinct_naver = sum(is_naver == "Y"),                
                .groups = "drop") 
  }) |> 
  inner_join(info_scrap, by = c("keyword_id")) |> 
  select(keyword_id, keyword, count, count_naver, distinct_cnt, distinct_naver) 


##==============================================================================
## 뉴스 기사 PDF 수집
##==============================================================================
target_pdf <- news_list |> 
  filter(as.Date(publish_date) == "2025-04-28") |> 
  filter(str_detect(link, "n.news.naver.com")) |> 
  select(keyword_id, keyword, title, link)

NROW(target_pdf) |> 
  seq() |> 
  purrr::walk(function(x) {
    Sys.sleep(2)
    keyword <- target_pdf |> 
      filter(row_number() == x) |> 
      pull(keyword)
    
    target_pdf |> 
      filter(row_number() == x) |> 
      pull(link) |> 
      koscrap::news2pdf(file_name = glue::glue("data/20250428/news_{keyword}_{x}.pdf"))
  })

