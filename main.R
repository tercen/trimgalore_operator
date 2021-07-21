library(tercen)
library(dplyr)

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

documentIds <- ctx$cselect("documentId")

file_names <- sapply(documentIds[[1]],
                     function(x) (ctx$client$fileService$get(x))$name) %>%
  sort()

if((length(file_names) %% 2) != 0) stop("Non-even number of files supplied. Are you sure you've supplied paired-end files?")


documentIds_to_output <- c()

for (first_in_pair_index in seq(1, length(file_names), by = 2)) {
  
  docIds = file_names[first_in_pair_index:(first_in_pair_index+1)]
  
  dodId_r1 <- names(docIds)[[1]]
  doc_r1 <- ctx$client$fileService$get(dodId_r1)
  filename_r1 <- docIds[[1]]
  writeBin(ctx$client$fileService$download(dodId_r1), filename_r1)
  on.exit(unlink(filename_r1))
  
  dodId_r2 <- names(docIds)[[2]]
  doc_r2 <- ctx$client$fileService$get(dodId_r2)
  filename_r2 <- docIds[[2]]
  writeBin(ctx$client$fileService$download(dodId_r2), filename_r2)
  on.exit(unlink(filename_r2))
  
  cmd <- paste("trimgalore --output_dir",
               paste0("output_dir_", first_in_pair_index),
               "--paired",
               filename_r1, filename_r2)
  
  for (filename in list.files(paste0("output_dir_", first_in_pair_index),
                          pattern = "*fq.gz",
                          full.names = TRUE)) {
    
    bytes = readBin(file(filename, 'rb'), 
                                raw(), 
                                n=file.info(filename)$size)
    
    fileDoc = FileDocument$new()
    fileDoc$name = filename
    fileDoc$projectId = doc_r1$projectId
    fileDoc$acl$owner = doc_r1$acl$owner
    fileDoc$size = length(bytes)
    
    fileDoc = ctx$client$fileService$upload(fileDoc, bytes)
   
    documentIds_to_output <- append(documentIds_to_output,
                                    fileDoc$id)
     
  }
}


(tibble(documentId = documentIds_to_output) %>%
    mutate(.ci = 0) %>%
    ctx$addNamespace() %>%
    ctx$save())
