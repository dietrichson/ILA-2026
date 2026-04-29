word_count <- function(path){
  my_text <- readLines(path)
  text_df <- tibble(line = 1:length(my_text), text = my_text)
  text_df |> 
  unnest_tokens(word, text) |> 
  nrow()
}

unique_word_count <- function(path, remove_stop = FALSE){
  my_text <- readLines(path)
  text_df <- tibble(line = 1:length(my_text), text = my_text)
  text_df |> 
    unnest_tokens(word, text) -> tmp
  
  if(isTRUE(remove_stop)){
     tmp <- tmp |> 
     anti_join(stop_words)
  }
    
     tmp |> count(word, sort = TRUE) |> 
    nrow()
}
