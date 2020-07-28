# convex

## Requirements
python > 3.6

## Set-up
To create an environment and install the required packages, type the following command:
<pre><code>conda env create -f environment.yml 
</code></pre>

## How to run the code
The app.py file takes 3 arguments: an S3 input path, an S3 output path, and a AWS profile name (optinal, the default value being 'default').

Example:
<pre><code>python app.py -i s3/input/path -o s3/output/path -p aws_profile_name 
</code></pre>

The app only works with buckets in the us-west-1 region.