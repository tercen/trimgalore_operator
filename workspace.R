library(tercen)
library(dplyr)

options("tercen.workflowId" = "0796038ab232707b473f109e77005e85")
options("tercen.stepId"     = "063e26df-16b3-4477-aefe-7bec98774754")

getOption("tercen.workflowId")
getOption("tercen.stepId")

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

documentIds <- ctx$cselect("documentId")

file_names <- sapply(documentIds[[1]],
                     function(x) (ctx$client$fileService$get(x))$name) %>%
  sort()

if((length(file_names) %% 2) != 0) stop("Non-even number of files supplied. Are you sure you've supplied paired-end files?")


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
               filename_r1,
               "--paired",
               filename_r1, filename_r2)
  
}



