# IndicatorQuantiles

## Purpose

The purpose of the indicator quantiles is to flag records that are "unusual" relative to all of the data in the DDI for a given indicator in order to facilitate QA/QC. 
They are not used to filter any of the data for SEACAR analyses, and the presence of a LowerQuantile or UpperQuantile flag on a DDI record 
alone does not necessarily indicate there is any issue with the record (neither does the absence of a LowerQuantile or UpperQuantile flag 
necessarily mean that a data record is correct).

## Relevant file locations

Current values can be found in the "LowQuantile" and "HighQuantile" columns of the "Ref_Parameters" worksheet.

The R script described below and the output files can be found in this repository.

## Process Steps

**IQ_Report_Render.R & IQ_Report.Rmd**  

1.  The *IQ_Report_Render.R* script lists all files in a given directory (default is a folder called "SEACARdata" located in the root folder) 
and filters it to a list of DDI exports to evaluate considering a list of parameters to skip (user-defined).  
2.  User sets the desired upper and lower quantile thresholds, as well as a number of standard deviations away from the mean to use for the calculations.  
3.  User sets the string value(s) in the DDI exports that should be considered as NA values.  
4.  The remainder of the script loops through the file list, returning the values listed below and binding them together by row into a 
single Excel spreadsheet that is saved to the User's working directory.  
5.  For each habitat included in the User's working directory a PDF report will be created in the "output" folder using *IQ_Report.Rmd*,
which provides an overview of questionable / flagged values.  
6.  In addition to the PDF reports, each habitat will provide a .txt data output file in the "output/data" folder containing questionable values.
