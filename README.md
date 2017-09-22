# Programming-the-data-flow-for-Big-Data-Analytics-using-Apache-Spark
Apache Spark environment: VM

Note 
- We are using python 3 to run the pyspark on VM. Add "export PYSPARK_PYTHON=python3" to .bashrc file on VM
- All the Inputs required for running the files and and corresponding outputs are present in the folders placed in each activity in the zip file submitted.

Task 1
----------
titanic.csv is the input for WelcomeToSparkWithPython.py
Input path is hardcoded as: /home/hadoop/sparkinput/titanic.csv

WelcomeOutput.txt is the MR output
Input path is hardcoded as: /home/hadoop/sparkoutput/WelcomeOutput.txt

The obtained output accuracies are the same as those from Spark notebook 

Featured Activity
--------------
We need to give 3 arguments while running the python file in Spark.
Usage: python_file.py <lemmatizer path> <input path> <output path>

Example command to run:
~/spark/bin/spark-submit ~/trialruns/wc3grams.py  ~/sparkinput/new_lemmatizer.csv ~/sparkinput/lucan.bellum_civile.part.1.tess ~/sparkoutput/

The inputs (latin tess files) for each task are found in corresponding input folder in the zip file submitted. put those tess files in VM and run Spark
We use the 2 tess files: lucan.bellum_civile.part.1.tess and vergil.aeneid.tess as input to the 2gram and 3gram python files. The corresponding output files have been submitted in the zip file.

Output format:
each line in the output file contains (n-gram, locations and count)
('wordx, wordy...', '[<Document#.chap#.line#.position#>...] count= # of Co-occurrence')

Sample Output for 2gram.py:
('quis si', '[<luc. 1.304.2>,<luc. 1.307.4>] count=2')
('credo aut', '[<luc. 1.493.3>] count=1')
('latium maculo', '[<luc. 1.105.2>] count=1')
('per orbis', '[<luc. 1.61.8>,<luc. 1.78.6>,<luc. 1.318.7>,<luc. 1.692.5>] count=4')
('et tacitum', '[<luc. 1.247.1>] count=1')

Runtime Results ploted graph for WordCount Bigrams vs Trigrams is shown in 2gramVs3gramPerformance.jpg

References:
https://stackoverflow.com/questions/35260584/how-to-find-bigram-frequency-on-a-rdd-using-python
