##==============================================================================
## 01. environments
##==============================================================================
##------------------------------------------------------------------------------
## 01.01. load libraries
##------------------------------------------------------------------------------
library(koscrap)
library(tidyverse)
library(readxl)

##------------------------------------------------------------------------------
## 01.02. set API key
##------------------------------------------------------------------------------
set_api_key("naver", 
            client_id = koscrap::get_api_key()$naver_client_id, 
            client_secret = koscrap::get_api_key()$naver_client_secret)

##------------------------------------------------------------------------------
## 01.03. set base date
##------------------------------------------------------------------------------
base_date <- "2025-06-05" # 기준일자
base_dir <- stringr::str_remove_all(base_date, "-")

if (!dir.exists(glue::glue("data/{base_dir}"))) {
  dir.create(glue::glue("data/{base_dir}"), recursive = TRUE)
}

##------------------------------------------------------------------------------
## 01.04. 메타정보 로드
##------------------------------------------------------------------------------
meta_file <- "meta/news_keywords.xlsx"
meta_keyword <- readxl::read_xlsx(meta_file, sheet = 2) 

target_id <- meta_keyword |> 
  filter(use_flag == "Y") |> 
  distinct(keyword_id) |> 
  pull() 


##==============================================================================
## 02. 뉴스 목록 수집
##==============================================================================
##------------------------------------------------------------------------------
## 02.01. 뉴스 목록 수집
##------------------------------------------------------------------------------
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

##------------------------------------------------------------------------------
## 02.02. 대상 뉴스 목록 정리
##------------------------------------------------------------------------------
info_scrap <- news_list |> 
  filter(as.Date(publish_date) == base_date) |> 
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

##------------------------------------------------------------------------------
## 02.03. 뉴스 목록 누적 집계
##------------------------------------------------------------------------------
result_stat <- target_id |> 
  purrr::map_df(function(id) {
    news_list |> 
      filter(keyword_id == id) |> 
      filter(as.Date(publish_date) == base_date) |> 
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
## 03. 뉴스 기사 PDF 수집
##==============================================================================
##------------------------------------------------------------------------------
## 03.01. PDF 수집 대상 뉴스 목록 도출
##------------------------------------------------------------------------------
target_pdf <- news_list |> 
  filter(as.Date(publish_date) == base_date) |> 
  filter(str_detect(link, "n.news.naver.com")) |> 
  select(keyword_id, keyword, title = title_text, link)

##------------------------------------------------------------------------------
## 03.02. PDF 수집 
##------------------------------------------------------------------------------
NROW(target_pdf) |> 
  seq() |> 
  purrr::walk(function(x) {
    Sys.sleep(2)
    gc()
    message(glue::glue("Collecting PDF for {x} of {NROW(target_pdf)}..."))
    
    keyword <- target_pdf |> 
      filter(row_number() == x) |> 
      pull(keyword)
    
    target_pdf |> 
      filter(row_number() == x) |> 
      pull(link) |> 
      koscrap::news2pdf_minimal(file_name = glue::glue("data/{base_dir}/news_{keyword}_{x}.pdf"))
  })


##------------------------------------------------------------------------------
## 03.03. PDF 파일 용량 축소
##------------------------------------------------------------------------------
tools::compactPDF(glue::glue("data/{base_dir}"),  gs_quality='ebook')



