# Data Engineering Exercise

This repo contains my solution to the take home data engineering job skills exercise.

## Table of Contents

- [Instructions](#instructions)
- [Installation](#installation)
- [Usage](#usage)
- [Report](#report)


## Instructions
You are provided with two datasets named file1.csv and file2.csv, these files contain the name of a restaurant and its address.

You are asked to write a script in Python to read both CSV files, match the entries between the two files, and harmonzie column names. Try to structure your code as a production ready pipeline.

In addition to the code in your project, please generate a CSV file and report.
The output CSV file should contain:
- The full list of restaurants and addresses present in any of the two files without duplicates
- A unique identifier for each restaurant
- The total number of occurrences of each entry
- Two boolean flags indicating whether the entry is present in file1 or file2


The report should briefly explain how the script merges the two files and generates the
additional output columns (IDs, counts, and flags). Explain how the script harmonizes addresses
and restaurant names to match them and how would your approach have changed if you had
more time to resolve the exercise.


## Installation
From the top level of this repo, where the requirements.txt file is, run the following command to install the required packages:

`pip install -r requirements.txt`


## Usage
To run the code and process the files run the following command from the top level of this repo:

`python src/main.py`

The source files and output file can be found in the data directory. This python script produces the output.csv file and will overwrite it each time it is run.
Please note that the file names are hardcoded and you will recieve and error if you do not run this from the top level of this repo.


## Report
My overall approach to this exercise was to clean and deduplicate the separate files before combining them into one file. I will highlight each major step that I took to generate my final output.

### Step 1: Read the source files
For the sake of this exercise I hardcoded the source files and their relative paths in a tuple as a global variable. In a production environment, I would never do this, but the exercise was about the data operations not reading and writing files. One alternative approach that I have used in the past is to specify a configuration file that can be modified to add or remove files. I did create the function that reads the source files in a way that more files could be added and it would continue to work. I did not have time to add error handling for if there were no source files configured or if reading the csv file was unsuccessful. This outputs a dataframe for each file that is read in.


### Step 2: Cleaning the data
I chose to clean and deduplicate the data in each file / dataframe before merging it together. To clean the data I first renamed the columns based on their position. When I was looking at each source file, I noticed that the columns where in the same order in both files. I realize that if the column order ever changed in the source files, this would cause an issue. Potentially, a more dynamic way to standardize the source columns would be to split and remove the numbers from the column names and then specify the explicit order for them.

Next I filled in any values that may have been missing. I used the term 'not provided' for any name, address, or city that was empty and a zip code of 00000. Because the zip column contained integers and not strings, I also created a list of the columns that are string datatypes so I can use them in certain cleaning operations. To ensure all the string values are the same case, I converted all string columns to lowercase. Then I removed any unncessary whitespace using the strip() function.

As I was cleaning up the file, I noticed that there were duplicate entires for some restaurants that had the same name, but one of the names included LLC, INC, or LTD. I refer to these as corporate suffixs, and removed these from the name column to make deduplication easier. I also noticed there were some symbols and signs that were in the name and address fields, which would make deduplication more difficult. I used a regex that matches on any character that is not a word or whitespace to remove them. The cleaning step modified records but it should not have removed any, so I check to make sure that the count from the dataframe that was passed in matches the count of the dataframe that is returned after cleaning and throw an exception if they are different. 


### Step 3: Deduplicate the data
This part was particularly difficult. Before deduplicating the data I needed to get a count of the occurence of each restaurant because the occurence would always be 1 once the data was deduplicated. To get the count of occurences I grouped the data by name, address, city, and zip. I chose to group the data this way because it is possible for a restaurant to have the same name but a different address and it is also possible for multiple restaurants to have the same addresses. I also specified the sorting so that in my next step I wouldn't lose the record with the correct occurence count. 

Next I deduplicated the data using the same grouping that I generated the occurence count with. Because of the sorting I did, I specified to keep the first occurence of each duplicate that was found. Once the duplicates are dropped I add in the boolean columns that specify the source. I pass in a string to the dedupe function that denotes the source file of the dataframe being deduplicated. Using the source string I am able to add both columns and the appropriate boolean values to the dataframe.

### Step 4: Combine the dataframes and generate a unique ID
Now that I had clean and deduplicated data, I concatanated the 2 dataframes into a single dataframe. I chose to concat instead of merge the dataframes to ensure that I preserved all of the data. I used a hash function to create a unique ID for each row. This ensures that no matter how many times you run this script you should always get the same unique ID for the same row assuming the rows contains the same data. Given the small dataset, I was not concerned with any type of hash collision. I also chose to move the ID column to the first position of the dataframe, because the first column is where I am used to seeing IDs.

### Step 5: Write the final dataframe
I didn't do anything special here, once again I hardcoded the file path and file name. I did not set this up in a separate function because I should always have only 1 dataframe to write as the output no matter how many files are used as input.


### Constraints and considerations
I believe that that output dataset is in better shape than it was originally in 2 separate files, but there is still cleaning and deduplicating that needs to be done at a more granular level that I did not have time to accomplish. Given more time, I would have done the following:
- Not hardcoded file names and paths for input or output.
- Increased error handling.
- Created a more dyanmic way of harmonizing the column names.
- Verified the address data, I believe google has a library that you can use to verify an address actually exists. This would also help in verifying the restaurant names.
- Further consolidated / cleaned restaurant names. I think I could do some type of fuzzy matching on the restaurant names to find even more duplicates in the list.
- The boolean columns seemed like a poor method of indicating source, I would have proposed including a source column that would hold the file name and/or the filepath instead of two boolean columns.
- I'm aware that other tools exist outside of Pandas that could have helped with this exercise. I would have explored using alternatives such as DuckDB to see if I could generate different, more accurate results or use them in combination with Pandas. 
- Included both unit and data quality / integrity tests where appropriate.
- Implemented this in a cloud environment. I think I could have set this up for free in AWS given their free tier for certain services.
- If I was able to get the data more clean I probably would have used the merge function to combine datasets instead of concat.
- I could have broken the dataset up into subsets based on name / address and merged them to get a single record for duplicates.