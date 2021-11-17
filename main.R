library(tercen)
library(dplyr)
library(stringr)

serialize.to.string = function(object){
  con = rawConnection(raw(0), "r+")
  saveRDS(object, con)
  str64 = base64enc::base64encode(rawConnectionValue(con))
  close(con)
  return(str64)
}
deserialize.from.string = function(str64){
  con = rawConnection(base64enc::base64decode(str64), "r+")
  object = readRDS(con)
  close(con)
  return(object)
}

find.schema.by.factor.name = function(ctx, factor.name){
  visit.relation = function(visitor, relation){
    if (inherits(relation,"SimpleRelation")){
      visitor(relation)
    } else if (inherits(relation,"CompositeRelation")){
      visit.relation(visitor, relation$mainRelation)
      lapply(relation$joinOperators, function(jop){
        visit.relation(visitor, jop$rightRelation)
      })
    } else if (inherits(relation,"WhereRelation") 
               || inherits(relation,"RenameRelation")){
      visit.relation(visitor, relation$relation)
    } else if (inherits(relation,"UnionRelation")){
      lapply(relation$relations, function(rel){
        visit.relation(visitor, rel)
      })
    } 
    invisible()
  }
  
  myenv = new.env()
  add.in.env = function(object){
    myenv[[toString(length(myenv)+1)]] = object$id
  }
  
  visit.relation(add.in.env, ctx$query$relation)
  
  schemas = lapply(as.list(myenv), function(id){
    ctx$client$tableSchemaService$get(id)
  })
  
  Find(function(schema){
    !is.null(Find(function(column) column$name == factor.name, schema$columns))
  }, schemas);
}


ctx <- tercenCtx()


schema <- find.schema.by.factor.name(ctx, names(ctx$cselect())[[1]])

table <- ctx$client$tableSchemaService$select(schema$id, Map(function(x) x$name, schema$columns), 0, schema$nRows)

table <- as_tibble(table)

hidden_colnames <- colnames(table)[str_starts(colnames(table), "\\.")]

is_paired_end <- as.character(ctx$op.value('paired_end'))

if (is_paired_end == "yes") {
  
  if (!((".forward_read_fastq_data" %in% hidden_colnames) &
        (".reverse_read_fastq_data" %in% hidden_colnames))) {
    
    stop("Input is not samples containing paired-end fastq data.")
    
  }
  
  
  output_table <- tibble()
  
  for (i in 1:nrow(table)) {
    
    sample_name <- select(table, ends_with(".sample"))[[1]][[i]]
    
    filename_r1 <- paste0(sample_name, "1.fastq.gz")
    filename_r2 <- paste0(sample_name, "2.fastq.gz")
    
    writeBin(deserialize.from.string(table[".forward_read_fastq_data"][[1]][[i]]), filename_r1)
    writeBin(deserialize.from.string(table[".reverse_read_fastq_data"][[1]][[i]]), filename_r2)
    
    cmd <- paste("trim_galore --output_dir",
                 paste0("output_dir_", i),
                 "--paired",
                 filename_r1, filename_r2)
    
    print(cmd)
    
    system(cmd)

    list.files(paste0("output_dir_", i))
        
    filename_val1 <- paste0("output_dir_", i, "/", sample_name, "1_val_1.fq.gz")
    
    print(filename_val1)
    bytes_val1 <- readBin(file(filename_val1, 'rb'),
                          raw(),
                          n=file.info(filename_val1)$size)
    
    filename_val2 <- paste0("output_dir_", i, "/", sample_name, "2_val_2.fq.gz")
    print(filename_val2)
    bytes_val2 <- readBin(file(filename_val2, 'rb'),
                          raw(),
                          n=file.info(filename_val2)$size)
    
    string_val1 <- serialize.to.string(bytes_val1)
    string_val2 <- serialize.to.string(bytes_val2)
    
    output_table <- bind_rows(output_table,
                              tibble(sample = sample_name,
                                     .forward_read_fastq_data = string_val1,
                                     .reverse_read_fastq_data = string_val2))
    
  }
  
  
} else if (is_paired_end == "no") {
  
  if (!(".single_end_fastq_data" %in% hidden_colnames)) {
    
    stop("Input is not samples containing single-end fastq data.")
    
  }
  
  
  output_table <- tibble()
  
  for (i in 1:nrow(table)) {
    
    sample_name <- select(table, ends_with(".sample"))[[1]][[i]]
    
    filename <- sample_name
    
    writeBin(deserialize.from.string(table[".single_end_fastq_data"][[1]][[i]]), filename)
    
    cmd <- paste("trim_galore --output_dir",
                 paste0("output_dir_", i),
                 filename)
    
    system(cmd)
    
    filename_trimmed <- list.files(paste0("output_dir_", i),
                                   pattern = "*fq.gz",
                                   full.names = TRUE)[[1]]
    
    bytes_trimmed <- readBin(file(filename_trimmed, 'rb'),
                             raw(),
                             n=file.info(filename_trimmed)$size)
    
    string_trimmed <- serialize.to.string(bytes_trimmed)
    
    output_table <- bind_rows(output_table,
                              tibble(sample = sample_name,
                                     .single_end_fastq_data = string_trimmed))
    
  }
  
}


save_output <- as.character(ctx$op.value('save_output_to_folder'))

if (save_output == "yes") {
  
  output_folder_prefix <- as.character(ctx$op.value('output_folder_prefix'))
  
  # create trim galore zipped output
  system("zip -r trim_galore_output.zip output_dir_*")
  
  # save zipped file to project folder
  filename <- "trim_galore_output.zip"
  bytes = readBin(file(filename, 'rb'),
                  raw(),
                  n = file.info(filename)$size)
  
  fileDoc = FileDocument$new()
  fileDoc$name = paste0(output_folder_prefix, "_", filename)
  fileDoc$projectId = ctx$cschema$projectId
  fileDoc$size = length(bytes)
  
  fileDoc = ctx$client$fileService$upload(fileDoc, bytes)
  
}

output_table %>%
  mutate(.ci = 1) %>%
  ctx$addNamespace() %>%
  ctx$save()

