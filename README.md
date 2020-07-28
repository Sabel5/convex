# convex

## Requirements
python > 3.6

## Set-up
To create an environment and install the required packages, type the following command:
<pre><code>conda env create -f environment.yml 
</code></pre>

Before running the code, activate the environment:
<pre><code>conda activate convex_env
</code></pre>

After running the code, deactivate the environment:
<pre><code>conda deactivate
</code></pre>

## How to run the code
The app.py file takes 3 arguments: an S3 input path, an S3 output path, and a AWS profile name (optinal, the default value is 'default').

Example:
<pre><code>python app.py -i s3a/input/path -o s3a/output/path -p aws_profile_name 
</code></pre>

The app only works with buckets in the us-west-1 region.